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
    protected val trieSetEmpty: TrieMap[(String, String), Boolean] = TrieMap.empty[(String, String), Boolean]

    private val facades: TrieMap[String, EntityFacade] =
      TrieMap.empty[String, EntityFacade]
    private val subscribers: TrieMap[String, TrieMap[(String, String), Boolean]] =
      TrieMap.empty[String, TrieMap[(String, String), Boolean]] // facade -> subscribers
//    protected val relations: TrieMap[String, TrieMap[String, Boolean]] =
//      TrieMap.empty[String, TrieMap[String, Boolean]] // facade -> relations

    def facade(entityId: String): Option[EntityFacade] = facades.get(entityId)
    def registerFacade(entityFacade: EntityFacade): Unit = facades.put(entityFacade.entity.id, entityFacade)

    def getSubscribers(entityId: String): Set[(String, String)] =
      subscribers.get(entityId).map(_.keySet.toSet[(String, String)]) getOrElse Set.empty[(String, String)]
//    override def getRelations(entityId: String): Set[String] =
//      relations.get(entityId).map(_.keySet.toSet[String]) getOrElse Set.empty[String]

    def addSubscriber(entityId: String, subscriberId: String, kind: String): Unit = {
      subscribers.putIfAbsent(entityId, trieSetEmpty)
      subscribers(entityId).update(subscriberId -> kind, true)

//      relations.putIfAbsent(entityId, trieSetEmpty)
//      relations(entityId).update(subscriberId, true)
    }
    def removeSubscriber(entityId: String, subscriberId: String, kind: String): Unit = {
      subscribers.get(entityId).foreach(_ -= (subscriberId -> kind))
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

  override def register(facade: EntityFacade)
                       (lastClock: facade.entity.ops.D#C,
                        relationClocks: Map[Entity, Any],
                        forcedSubscribers: Set[EntityFacade]): Future[Unit] = {
    innerStorage.registerFacade(facade)

    // this facade depends on that relations
    relationClocks.foreach { case (e, clock) => subscribe(e, facade.entity.id, facade.entity.ops.kind, Some(clock)) }

    // TODO: request facade to approve all subscribers


    // sync registered entity clock
    storage.getLastClock(facade.entity).flatMap { lastStoredClockOpt =>
      val fops: facade.entity.ops.type = facade.entity.ops

      lastStoredClockOpt.flatMap(fops.matchClock).map(lastStoredClock =>
        if (fops.ordering.gt(lastClock, lastStoredClock))
          facade.getUpdatesFrom(lastStoredClock).flatMap(
            dataUpdated(facade.entity.id, _, relationClocks.keySet, Set.empty, forcedSubscribers)
          )
        else Future.unit
      ).getOrElse(storage.increase(facade.entity)(lastClock))
    }
  }

  def dataUpdated(entityId: String, // TODO: may be EntityFacade instead?
                  _data: Data,
                  addedRelations: Set[Entity],
                  removedRelations: Set[Entity],
                  forcedSubscribers: Set[EntityFacade]): Future[Unit] = {
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

          // TODO: move it to LocalDataStorage?
          removedRelations.foreach(unsubscribe(_, facade.entity.id, facade.entity.kind))
          addedRelations.foreach(subscribe(_, facade.entity.id, facade.entity.ops.kind, None))
        }
      }
    }
  }

  def syncRelationClocks(entityId: String, entityKind: String, relationClocks: Map[Entity, Any]): Future[Unit] = {
    relationClocks.foreach { case (relation, clock) =>
      innerStorage.facade(relation.id)
        .orElse(relation.ops.createFacadeFromEntityId(relation.id))
        .foreach(syncData(_, entityId, entityKind, Some(clock)))
    }

    Future.unit
  }

  // TODO: private within pacakge
  def notifySubscribers(entity: Entity, forcedSubscribers: Set[EntityFacade])(data: entity.ops.D): Unit = {
    forcedSubscribers.foreach(f => sendChangeToOne(entity, f.entity.id, f.entity.kind)(data))
    innerStorage.getSubscribers(entity.id).foreach { case (subscriberId, kind) =>
      sendChangeToOne(entity, subscriberId, kind)(data)
    }
  }

  protected def subscribeApproved(entityFacade: EntityFacade,
                                  subscriberId: String,
                                  subscriberKind: String,
                                  lastKnownDataClockOpt: Option[Any]): Future[Unit] = {
    innerStorage.addSubscriber(entityFacade.entity.id, subscriberId, subscriberKind)
    syncData(entityFacade, subscriberId, subscriberKind, lastKnownDataClockOpt)

    Future.unit
  }

  def sendChangeToOne(entity: Entity, subscriberId: String, subscriberKind: String)
                               (entityData: entity.ops.D): Option[Future[Unit]] =
    innerStorage.facade(subscriberId)
      .orElse(DataOperationsRegistry.getOps(subscriberKind).createFacadeFromEntityId(subscriberId))
      .map(_.onUpdate(entity.id, entityData))

  private def syncData(entityFacade: EntityFacade,
                       subscriberId: String,
                       subscriberKind: String,
                       lastKnownDataClockOpt: Option[Any]): Unit = {
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
          sendChangeToOne(entityFacade.entity, subscriberId, subscriberKind)(d)
        )
    })
  }

  def subscribe(entity: Entity, // this must be entity to get ops and compare clocks
                subscriberId: String,
                subscriberKind: String,
                lastKnownDataClockOpt: Option[Any]): Unit = {
    log.debug("subscribe {}, {}, {}", subscriberId, entity.id, innerStorage.facade(entity.id))

    // we must subscribe it, otherways subscriber will not receive any changes from entity while it is not registered
    // TODO: we need to compare stored and lastKnown clocks and force entity start only if it need it
    innerStorage.facade(entity.id)
      .orElse(entity.ops.createFacadeFromEntityId(entity.id))
      .foreach(entityFacade =>
        entityFacade.requestForApprove(subscriberId, subscriberKind).map(
          if (_) subscribeApproved(entityFacade, subscriberId, subscriberKind, lastKnownDataClockOpt)
          else log.warn("Failed to subscribe on {} due untrusted kind {}{}", entity.id, subscriberKind, "")
        )
      )
  }

  // TODO: add test
  def unsubscribe(entity: Entity, subscriberId: String, kind: String): Unit = {
    log.debug("Unsubscribe entity {} from relation {}{}", subscriberId, entity.id, "")

    innerStorage.removeSubscriber(entity.id, subscriberId, kind: String)
  }
}
