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

//  trait InnerDataStorage {
//    def facade(entityId: String): Option[EntityFacade]
//    def registerFacade(entityFacade: EntityFacade): Unit
//
//    def getSubscribers(entityId: String): Set[String]
//
//    def addSubscriber(entityId: String, subscriberId: String): Unit
//    def removeSubscriber(entityId: String, subscriberId: String): Unit
//  }

  class MemoryFallbackStorage(storage: Storage)(implicit ec: ExecutionContext) extends MemoryStorage {
    override def increase(entity: Entity)(dataClock: entity.ops.D#C): Future[Unit] =
      super.increase(entity)(dataClock).flatMap(_ => storage.increase(entity)(dataClock))

    override def getLastClock(entity: Entity): Future[Option[entity.ops.D#C]] =
      storage.getLastClock(entity).flatMap {
        case None => super.getLastClock(entity)
        case opt => Future.successful(opt)
      }
  }

  private class MemoryInnerStorage /*extends InnerDataStorage */{
    protected val trieSetEmpty: TrieMap[String, Boolean] = TrieMap.empty[String, Boolean]

    private val facades: TrieMap[String, EntityFacade] =
      TrieMap.empty[String, EntityFacade]
    private val subscribers: TrieMap[String, TrieMap[String, Boolean]] =
      TrieMap.empty[String, TrieMap[String, Boolean]] // facade -> subscribers
//    protected val relations: TrieMap[String, TrieMap[String, Boolean]] =
//      TrieMap.empty[String, TrieMap[String, Boolean]] // facade -> relations

    def facade(entityId: String): Option[EntityFacade] = facades.get(entityId)
    def registerFacade(entityFacade: EntityFacade): Unit = facades.put(entityFacade.entity.id, entityFacade)

    def getSubscribers(entityId: String): Set[String] =
      subscribers.get(entityId).map(_.keySet.toSet[String]) getOrElse Set.empty[String]
//    override def getRelations(entityId: String): Set[String] =
//      relations.get(entityId).map(_.keySet.toSet[String]) getOrElse Set.empty[String]

    def addSubscriber(entityId: String, subscriberId: String): Unit = {
      subscribers.putIfAbsent(entityId, trieSetEmpty)
      subscribers(entityId).update(subscriberId, true)

//      relations.putIfAbsent(entityId, trieSetEmpty)
//      relations(entityId).update(subscriberId, true)
    }
    def removeSubscriber(entityId: String, subscriberId: String): Unit = {
      subscribers.get(entityId).foreach(_ -= subscriberId)
      subscribers.remove(entityId, trieSetEmpty)

//      relations.get(entityId).foreach(_ -= relationId)
//      relations.remove(entityId, trieSetEmpty)
    }
  }
}
import AsyncDatahub._

class AsyncDatahub(_storage: Storage)
                  (implicit val ec: ExecutionContext) extends Datahub[Future] {
  private val storage = new MemoryFallbackStorage(_storage)(ec)
  private val innerStorage = new MemoryInnerStorage
  protected val log = LoggerFactory.getLogger(getClass)
  private implicit val timeout: FiniteDuration = 3.seconds

  def register(facade: EntityFacade)
              (lastClock: facade.entity.ops.D#C, relationClocks: Map[String, Any]): Future[Unit] = {
    innerStorage.registerFacade(facade)

    // this facade depends on that relations
    relationClocks.foreach { case (id, clock) => subscribe(facade, id, Some(clock)) }

    // TODO: request facade to approve all subscribers


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

  def dataUpdated(entityId: String, // TODO: may be EntityFacade instead?
                  _data: Data,
                  addedRelations: Set[String],
                  removedRelations: Set[String],
                  forcedSubscribers: Set[String]): Future[Unit] = {
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
          notifySubscribers(facade.entity, forcedSubscribers)(data)

          removedRelations.foreach(unsubscribe(facade, _))
          addedRelations.foreach(subscribe(facade, _, None))
        }
      }
    }
  }

  def syncRelationClocks(entityId: String, relationClocks: Map[String, Any]): Future[Unit] = {
    relationClocks.foreach { case (rid, clock) =>
      syncData(entityId, rid, Some(clock))
    }

    Future.unit
  }

  protected def notifySubscribers(entity: Entity)(data: entity.ops.D): Unit =
    innerStorage
      .getSubscribers(entity.id) // TODO: + forced subscribers
      .foreach(subscriberId => sendChangeToOne(subscriberId, entity)(data))

  protected def subscribeApproved(entityFacade: EntityFacade,
                                  subscriberId: String,
                                  lastKnownDataClockOpt: Option[Any]): Future[Unit] = {
    innerStorage.addSubscriber(entityFacade.entity.id, subscriberId)
    syncData(entityFacade, subscriberId, lastKnownDataClockOpt)

    Future.unit
  }

  protected def sendChangeToOne(toId: String, relation: Entity)(relationData: relation.ops.D): Future[Unit] =
    innerStorage.facade(toId).map(_.onUpdate(relation.id, relationData)) getOrElse Future.unit

  private def syncData(entityFacade: EntityFacade, subscriberId: String, lastKnownDataClockOpt: Option[Any]): Unit = {
    log.debug(
      "Subscribe entity {} on {} with last known relation clock {}",
      subscriberId, entityFacade.entity.id, lastKnownDataClockOpt
    )

    val entityOps: entityFacade.entity.ops.type = entityFacade.entity.ops

    storage.getLastClock(entityFacade.entity).foreach(_.flatMap(entityOps.matchClock).foreach { lastClock =>

      val lastKnownDataClock = lastKnownDataClockOpt.flatMap(entityOps.matchClock) getOrElse entityOps.zero.clock

      log.debug("lastClock {}, lastKnownClock {}, {}",
        Seq(lastClock, lastKnownDataClock, entityOps.ordering.gt(lastClock, lastKnownDataClock))
      )

      if (entityOps.ordering.gt(lastClock, lastKnownDataClock))
        entityFacade.getUpdatesFrom(lastKnownDataClock).foreach(d =>
          sendChangeToOne(subscriberId, entityFacade.entity)(d)
        )
    })
  }

  protected def subscribe(entityId: String,
                          subscriberId: String,
                          subscriberKind: String,
                          lastKnownDataClockOpt: Option[Any]): Unit = {
    log.debug("subscribe {}, {}, {}", subscriberId, entityId, innerStorage.facade(entityId))

    innerStorage.facade(entityId).map(entityFacade =>
      entityFacade.requestForApprove(subscriberId, subscriberKind).map(
        if (_) subscribeApproved(entityFacade, subscriberId, lastKnownDataClockOpt)
        else log.warn("Failed to subscribe on {} due untrusted kind {}{}", entityId, subscriberKind, "")
      )
    ) getOrElse innerStorage.addSubscriber(entityId, subscriberId)
  }

  // TODO: add test
  protected def unsubscribe(entityId: String, subscriberId: String): Unit = {
    log.debug("Unsubscribe entity {} from relation {}{}", subscriberId, entityId, "")

    innerStorage.removeSubscriber(subscriberId, entityId)
  }
}
