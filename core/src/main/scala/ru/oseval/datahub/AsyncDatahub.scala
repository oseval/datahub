package ru.oseval.datahub

import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

object AsyncDatahub {
  private[datahub] class MemoryInnerStorage {
    private val trieSetEmpty: TrieMap[Subscriber, Boolean] = TrieMap.empty[Subscriber, Boolean]
    private val facades: TrieMap[String, LocalEntityFacade] =
      TrieMap.empty[String, LocalEntityFacade]
    private val remoteFacades: TrieMap[String, RemoteEntityFacade] =
      TrieMap.empty[String, RemoteEntityFacade]
    private val subscribers: TrieMap[String, TrieMap[Subscriber, Boolean]] =
      TrieMap.empty[String, TrieMap[Subscriber, Boolean]] // facade -> subscribers

    def facade(entity: Entity): Option[EntityFacade] = facades.get(entity.id).orElse(remoteFacades.get(entity.ops.kind))
    def registerFacade(entityFacade: EntityFacade): Unit = entityFacade match {
      case f: LocalEntityFacade => facades.put(f.entity.id, f)
      case f: RemoteEntityFacade => remoteFacades.put(f.ops.kind, f)
    }

    def getSubscribers(entityId: String): Set[Subscriber] =
      subscribers.get(entityId).map(_.keySet.toSet[Subscriber]) getOrElse Set.empty[Subscriber]

    def addSubscriber(entityId: String, subscriber: Subscriber): Unit = {
      subscribers.putIfAbsent(entityId, trieSetEmpty)
      subscribers(entityId).update(subscriber, true)

    }
    def removeSubscriber(entity: Entity, subscriber: Subscriber): Unit = {
      subscribers.get(entity.id).foreach(_ -= subscriber)
      subscribers.remove(entity.id, trieSetEmpty)
    }
  }
}
import AsyncDatahub._

// TODO: AsyncDatahub(store: SubscriptionsStore)
class AsyncDatahub()(implicit val ec: ExecutionContext) extends Datahub {
  protected val innerStorage = new MemoryInnerStorage
  protected val log = LoggerFactory.getLogger(getClass)

  override def register(facade: EntityFacade): Unit =
    innerStorage.registerFacade(facade)

  def dataUpdated(entity: Entity)(data: entity.ops.D): Unit =
    innerStorage.getSubscribers(entity.id).foreach(sendChangeToOne(entity, _)(data))

  def syncRelationClocks(subscriber: Subscriber, relationClocks: Map[Entity, Any]): Unit =
    relationClocks.foreach { case (relation, clock) =>
      innerStorage.facade(relation).foreach(syncData(_, relation, subscriber, Some(clock)))
    }

  def sendChangeToOne(entity: Entity, subscriber: Subscriber)
                     (entityData: entity.ops.D): Unit =
    subscriber.onUpdate(entity)(entityData)

  def subscribe(entity: Entity,
                subscriber: Subscriber,
                lastKnownDataClock: Any): Boolean = {
    log.debug("subscribe {}, {}, {}", subscriber, entity.id, innerStorage.facade(entity))

    innerStorage.addSubscriber(entity.id, subscriber)

    innerStorage.facade(entity).map { f =>
      f match { case rem: RemoteEntityFacade => rem.onSubscribe(entity, subscriber, lastKnownDataClock) case _ => }
      syncData(f, entity, subscriber, Some(lastKnownDataClock))
    }.isDefined
  }

  protected def syncData(entityFacade: EntityFacade,
                         entity: Entity,
                         subscriber: Subscriber,
                         lastKnownDataClockOpt: Option[Any]): Unit = {
    log.debug(
      "Subscribe on {} with last known relation clock {}",
      Seq(entity.id, lastKnownDataClockOpt)
    )

    // TODO: optimize this on facade level (e.g. store last clock inside global facade entity
    (entityFacade match {
      case f: LocalEntityFacade =>
        val ops: f.entity.ops.type = f.entity.ops
        val lastKnownDataClock = lastKnownDataClockOpt.flatMap(ops.matchClock) getOrElse ops.zero.clock
        f.syncData(lastKnownDataClock)
      case f: RemoteEntityFacade =>
        val ops: f.ops.type = f.ops
        val lastKnownDataClock = lastKnownDataClockOpt.flatMap(f.ops.matchClock) getOrElse ops.zero.clock
        f.syncData(entity.id, lastKnownDataClock)
    })/*.map(d =>
      entity.ops.matchData(d).foreach(sendChangeToOne(entity, subscriber))
    )*/
  }

  // TODO: add test
  def unsubscribe(entity: Entity, subscriber: Subscriber): Unit = {
    log.debug("Unsubscribe from relation {}", entity.id)

    innerStorage.removeSubscriber(entity, subscriber)
  }
}
