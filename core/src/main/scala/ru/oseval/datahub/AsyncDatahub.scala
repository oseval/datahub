package ru.oseval.datahub

import org.slf4j.LoggerFactory
import ru.oseval.datahub.data.Data

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

object AsyncDatahub {
  trait Storage {
    def increase(entity: Entity)(dataClock: entity.ops.D#C): Future[Unit]
    def getLastClock(entity: Entity): Future[Option[entity.ops.D#C]]
  }

  trait InnerDataStorage {
    def facade(entityId: String): Option[EntityFacade]
    def registerFacade(entityFacade: EntityFacade): Unit

    def getSubscribers(entityId: String): Set[String]
    def getRelations(entityId: String): Set[String]

    def addRelation(entityId: String, subscriberId: String): Unit
    def removeRelation(entityId: String, subscriberId: String): Unit

    def setForcedSubscribers(entityId: String, subscribers: Set[EntityFacade]): Unit
  }

  class MemoryFallbackStorage(storage: Storage)(implicit ec: ExecutionContext) extends MemoryStorage {
    override def increase(entity: Entity)(dataClock: entity.ops.D#C): Future[Unit] =
      super.increase(entity)(dataClock).flatMap(_ => storage.increase(entity)(dataClock))

    override def getLastClock(entity: Entity): Future[Option[entity.ops.D#C]] =
      storage.getLastClock(entity).flatMap {
        case None => super.getLastClock(entity)
        case opt => Future.successful(opt)
      }
  }

  class MemoryInnerStorage extends InnerDataStorage {
    private val trieSetEmpty: TrieMap[String, Boolean] = TrieMap.empty[String, Boolean]

    private val facades: TrieMap[String, EntityFacade] =
      TrieMap.empty[String, EntityFacade]
    private val subscribers: TrieMap[String, TrieMap[String, Boolean]] =
      TrieMap.empty[String, TrieMap[String, Boolean]] // facade -> subscribers
    private val relations: TrieMap[String, TrieMap[String, Boolean]] =
      TrieMap.empty[String, TrieMap[String, Boolean]] // facade -> relations

    // subscribers to which changes will be sent anyway, but without clock sync
    private val forcedSubscribers: TrieMap[String, Set[EntityFacade]] =
      new TrieMap[String, Set[EntityFacade]]

    override def facade(entityId: String): Option[EntityFacade] = facades.get(entityId)
    override def registerFacade(entityFacade: EntityFacade): Unit = facades.put(entityFacade.entity.id, entityFacade)

    override def getSubscribers(entityId: String): Set[String] =
      subscribers.get(entityId).map(_.keySet.toSet[String]) getOrElse Set.empty[String]
    override def getRelations(entityId: String): Set[String] =
      relations.get(entityId).map(_.keySet.toSet[String]) getOrElse Set.empty[String]

    override def addRelation(entityId: String, relationId: String): Unit = {
      subscribers.putIfAbsent(relationId, trieSetEmpty)
      subscribers(relationId).update(entityId, true)

      relations.putIfAbsent(entityId, trieSetEmpty)
      relations(entityId).update(relationId, true)
    }
    override def removeRelation(entityId: String, relationId: String): Unit = {
      subscribers.get(relationId).foreach(_ -= entityId)
      subscribers.remove(relationId, trieSetEmpty)

      relations.get(entityId).foreach(_ -= relationId)
      relations.remove(entityId, trieSetEmpty)
    }

    override def setForcedSubscribers(entityId: String, subscribers: Set[EntityFacade]): Unit =
      forcedSubscribers.update(entityId, subscribers)
  }
}
import AsyncDatahub._

class AsyncDatahub(_storage: Storage, protected val innerStorage: InnerDataStorage)
                  (implicit val ec: ExecutionContext) extends Datahub[Future] {
  private val storage = new MemoryFallbackStorage(_storage)(ec)
  private val log = LoggerFactory.getLogger(getClass)
  private implicit val timeout: FiniteDuration = 3.seconds

  def register(facade: EntityFacade)
              (lastClock: facade.entity.ops.D#C, relationClocks: Map[String, Any]): Future[Unit] = {
    innerStorage.registerFacade(facade)

    // this facade depends on that relations
    relationClocks.foreach { case (id, clock) => subscribe(facade, id, Some(clock)) }

    // sync registered entity clock
    storage.getLastClock(facade.entity).flatMap { lastStoredClockOpt =>
      val fops: facade.entity.ops.type = facade.entity.ops

      lastStoredClockOpt.flatMap(fops.matchClock).map(lastStoredClock =>
        if (fops.ordering.gt(lastClock, lastStoredClock))
          facade.getUpdatesFrom(lastStoredClock).flatMap(dataUpdated(facade.entity.id, _))
        else Future.unit
      ).getOrElse(storage.increase(facade.entity)(lastClock))
    }
  }

  def setForcedSubscribers(entityId: String, forced: Set[EntityFacade]): Future[Unit] =
    Future.successful(innerStorage.setForcedSubscribers(entityId, forced))

  def dataUpdated(entityId: String, _data: Data): Future[Unit] = {
    innerStorage.facade(entityId).fold(
      Future.failed[Unit](new Exception("Facade with id=" + entityId + " is not registered"))
    ) { facade =>
      facade.entity.ops.matchData(_data).fold(
        Future.failed[Unit](new Exception(
          "Entity " + entityId + " with taken facade " +
            facade.entity.id + " does not match data " + _data.getClass.getName
        ))
      ) { data =>
        storage.increase(facade.entity)(data.clock).map { _ =>
          // who subscribed on that facade
          innerStorage.getSubscribers(facade.entity.id)
            .flatMap(innerStorage.facade)
            .foreach(f => sendChangeToOne(f, facade.entity)(data))
          // the facades on which that facade depends
          val relatedFacades = facade.entity.ops.getRelations(data)
          val oldRelations = innerStorage.getRelations(entityId)
          (oldRelations -- relatedFacades).foreach(unsubscribe(facade, _))
          (relatedFacades -- oldRelations).foreach(subscribe(facade, _, None))
        }
      }
    }
  }

  def syncRelationClocks(entityId: String, relationClocks: Map[String, Any]): Future[Unit] = {
    innerStorage.facade(entityId).foreach { f =>
      relationClocks.foreach { case (rid, clock) => syncRelation(f, rid, Some(clock)) }
    }

    Future.unit
  }

  private def subscribeApproved(facade: EntityFacade, relationId: String, lastKnownDataClockOpt: Option[Any]): Future[Unit] = {
    innerStorage.addRelation(facade.entity.id, relationId)
    syncRelation(facade, relationId, lastKnownDataClockOpt)

    Future.unit
  }

  protected def sendChangeToOne(to: EntityFacade, relation: Entity)(relationData: relation.ops.D): Future[Unit] =
    to.onUpdate(relation.id, relationData)

  private def syncRelation(facade: EntityFacade, relationId: String, lastKnownDataClockOpt: Option[Any]): Unit = {
    innerStorage.facade(relationId).foreach { relation =>
      log.debug("Subscribe entity {} on {} with last known relation clock {}", facade.entity.id, relationId, lastKnownDataClockOpt)

      val relops: relation.entity.ops.type = relation.entity.ops

      storage.getLastClock(relation.entity).foreach(_.flatMap(relops.matchClock).foreach { lastClock =>

        val lastKnownDataClock = lastKnownDataClockOpt.flatMap(relops.matchClock) getOrElse relops.zero.clock

        log.debug("lastClock {}, lastKnownClock {}, {}",
          Seq(lastClock, lastKnownDataClock, relops.ordering.gt(lastClock, lastKnownDataClock))
        )

        if (relops.ordering.gt(lastClock, lastKnownDataClock))
          relation.getUpdatesFrom(lastKnownDataClock).foreach(d =>
            sendChangeToOne(facade, relation.entity)(d)
          )
      })
    }
  }

  private def subscribe(facade: EntityFacade, relatedId: String, lastKnownDataClockOpt: Option[Any]): Unit = {
    log.debug("subscribe {}, {}, {}", facade.entity.id, relatedId, innerStorage.facade(relatedId))

    innerStorage.facade(relatedId).foreach(relation =>
      relation.requestForApprove(facade.entity).map(
        if (_) subscribeApproved(facade, relatedId, lastKnownDataClockOpt)
        else log.warn("Failed to subscribe on {} due untrusted kind {}{}", relatedId, facade.entity.ops.kind, "")
      )
    )
  }

  // TODO: add test
  private def unsubscribe(facade: EntityFacade, relationId: String): Unit = {
    log.debug("Unsubscribe entity {} from related {}", Seq(facade.entity.id, relationId): _*)

    innerStorage.removeRelation(facade.entity.id, relationId)
  }
}
