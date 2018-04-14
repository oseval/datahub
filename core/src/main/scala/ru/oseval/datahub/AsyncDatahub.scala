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

  private[datahub] class MemoryInnerStorage /*extends InnerDataStorage */{
    private val trieSetEmpty: TrieMap[Entity, Boolean] = TrieMap.empty[Entity, Boolean]
    private val facades: TrieMap[String, EntityFacade] =
      TrieMap.empty[String, EntityFacade]
    private val subscribers: TrieMap[String, TrieMap[Entity, Boolean]] =
      TrieMap.empty[String, TrieMap[Entity, Boolean]] // facade -> subscribers

    def facade(entityId: String): Option[EntityFacade] = facades.get(entityId)
    def registerFacade(entityFacade: EntityFacade): Unit = facades.put(entityFacade.entity.id, entityFacade)

    def getSubscribers(entityId: String): Set[Entity] =
      subscribers.get(entityId).map(_.keySet.toSet[Entity]) getOrElse Set.empty[Entity]

    def addSubscriber(entityId: String, subscriber: Entity): Unit = {
      subscribers.putIfAbsent(entityId, trieSetEmpty)
      subscribers(entityId).update(subscriber, true)

    }
    def removeSubscriber(entityId: String, subscriber: Entity): Unit = {
      subscribers.get(entityId).foreach(_ -= subscriber)
      subscribers.remove(entityId, trieSetEmpty)
    }
  }
}
import AsyncDatahub._

class AsyncDatahub(_storage: Storage, repeater: Repeater)
                  (implicit val ec: ExecutionContext) extends Datahub[Future] {
  private val storage = new MemoryFallbackStorage(_storage)(ec)
  protected val innerStorage = new MemoryInnerStorage
  protected val log = LoggerFactory.getLogger(getClass)
  protected implicit val timeout: FiniteDuration = 3.seconds

  // TODO: perhaps this is not expected
  def facade(entity: Entity): Option[EntityFacade] = innerStorage.facade(entity.id).orElse(
    entity.ops.createFacadeFromEntityId(entity.id)
  )

  override def register(facade: EntityFacade)
                       (lastClock: facade.entity.ops.D#C,
                        relationClocks: Map[Entity, Any],
                        forcedSubscribers: Set[EntityFacade]): Future[Unit] = {
    innerStorage.registerFacade(facade)

    // this facade depends on that relations
    val subscribeOnRelations = Future.traverse(relationClocks) { case (e, clock) =>
      subscribe(e, facade.entity, Some(clock))
    }

    // TODO: request facade to approve all subscribers

    // sync registered entity clock
    val syncFacade = repeater.run(() =>
      storage.getLastClock(facade.entity).flatMap { lastStoredClockOpt =>
        val fops: facade.entity.ops.type = facade.entity.ops

        lastStoredClockOpt.flatMap(fops.matchClock).map(lastStoredClock =>
          if (fops.ordering.gt(lastClock, lastStoredClock))
            facade.getUpdatesFrom(lastStoredClock).flatMap(
              dataUpdated(facade.entity, forcedSubscribers)
            )
          else Future.unit
        ).getOrElse(storage.increase(facade.entity)(lastClock))
      }
    )

    subscribeOnRelations.flatMap(_ => syncFacade)
  }

  def dataUpdated(entity: Entity, forcedSubscribers: Set[EntityFacade])(data: entity.ops.D): Future[Unit] =
    repeater.run(() => storage.increase(entity)(data.clock)).map(_ =>
      notifySubscribers(entity, forcedSubscribers)(data)
    )

  def syncRelationClocks(entity: Entity, relationClocks: Map[Entity, Any]): Future[Unit]=
    Future.traverse(
      relationClocks.flatMap { case (relation, clock) =>
        facade(relation).map(_ -> clock)
      }
    ) { case (relationFacade, clock) =>
      syncData(relationFacade, entity, Some(clock))
    }.map(_ => ())

  def notifySubscribers(entity: Entity, forcedSubscribers: Set[EntityFacade])(data: entity.ops.D): Unit = {
    forcedSubscribers.foreach(f => sendChangeToOne(entity, f.entity)(data))
    println(("SSSS", entity, innerStorage.getSubscribers(entity.id), data))
    innerStorage.getSubscribers(entity.id).foreach(sendChangeToOne(entity, _)(data))
  }

  protected def subscribeApproved(entityFacade: EntityFacade,
                                  subscriber: Entity,
                                  lastKnownDataClockOpt: Option[Any]): Unit = {
    innerStorage.addSubscriber(entityFacade.entity.id, subscriber)
    syncData(entityFacade, subscriber, lastKnownDataClockOpt)
  }

  def sendChangeToOne(entity: Entity, subscriber: Entity)
                     (entityData: entity.ops.D): Option[Future[Unit]] =
    facade(subscriber).map(_.onUpdate(entity.id, entityData))

  protected def syncData(entityFacade: EntityFacade,
                         subscriber: Entity,
                         lastKnownDataClockOpt: Option[Any]): Future[Unit] = {
    log.debug(
      "Subscribe entity {} on {} with last known relation clock {}",
      subscriber.id, entityFacade.entity.id, lastKnownDataClockOpt
    )

    val entityOps: entityFacade.entity.ops.type = entityFacade.entity.ops

    repeater.run(() => storage.getLastClock(entityFacade.entity))
      .flatMap(_.flatMap(entityOps.matchClock).map { lastClock =>

        val lastKnownDataClock = lastKnownDataClockOpt.flatMap(entityOps.matchClock) getOrElse entityOps.zero.clock

        log.debug("lastClock {}, lastKnownClock {}, {}",
          Seq(lastClock, lastKnownDataClock, entityOps.ordering.gt(lastClock, lastKnownDataClock))
        )

        if (entityOps.ordering.gt(lastClock, lastKnownDataClock))
          repeater.run(() => entityFacade.getUpdatesFrom(lastKnownDataClock).flatMap(d =>
            sendChangeToOne(entityFacade.entity, subscriber)(d) getOrElse Future.unit
          ))
        else
          Future.unit
      } getOrElse Future.unit)
  }

  // TODO: ADD SUBSCRIBED ON TO FACADE!!!
  def subscribe(entity: Entity, // this must be entity to get ops and compare clocks
                subscriber: Entity,
                lastKnownDataClockOpt: Option[Any]): Future[Unit] = {
    log.debug("subscribe {}, {}, {}", subscriber, entity.id, facade(entity))

    // TODO: we need to compare stored and lastKnown clocks and force entity start only if it need it
    println(("AAAAA", entity, facade(entity)))
    facade(entity).map { entityFacade =>
      println(("FACADE", entityFacade.entity, entityFacade))

      repeater.run(() =>
        entityFacade.requestForApprove(subscriber).map(
          if (_) subscribeApproved(entityFacade, subscriber, lastKnownDataClockOpt)
          else log.warn("Failed to subscribe on {} due untrusted kind {}{}", entity.id, subscriber.ops.kind, "")
        )
      )
    }.getOrElse {
      // we must subscribe it, otherway subscriber will not receive any changes from entity when it will be registered
      innerStorage.addSubscriber(entity.id, subscriber)
      Future.unit
    }
  }

  // TODO: add test
  def unsubscribe(entity: Entity, subscriber: Entity): Future[Unit] = {
    log.debug("Unsubscribe entity {} from relation {}{}", subscriber.id, entity.id, "")

    innerStorage.removeSubscriber(entity.id, subscriber)

    Future.unit
  }
}
