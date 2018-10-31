package ru.oseval.datahub

import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

object AsyncDatahub {
  private[datahub] class MemoryInnerStorage {
    private val trieSetEmpty: TrieMap[Subscriber, Boolean] = TrieMap.empty[Subscriber, Boolean]
    private val facades: TrieMap[String, EntityFacade] =
      TrieMap.empty[String, EntityFacade]
    private val subscribers: TrieMap[String, TrieMap[Subscriber, Boolean]] =
      TrieMap.empty[String, TrieMap[Subscriber, Boolean]] // facade -> subscribers

    def facade(entityId: String): Option[EntityFacade] = facades.get(entityId)
    def registerFacade(entityFacade: EntityFacade): Unit = facades.put(entityFacade.entity.id, entityFacade)

    def getSubscribers(entityId: String): Set[Subscriber] =
      subscribers.get(entityId).map(_.keySet.toSet[Subscriber]) getOrElse Set.empty[Subscriber]

    def addSubscriber(entityId: String, subscriber: Subscriber): Unit = {
      subscribers.putIfAbsent(entityId, trieSetEmpty)
      subscribers(entityId).update(subscriber, true)

    }
    def removeSubscriber(entityId: String, subscriber: Subscriber): Unit = {
      subscribers.get(entityId).foreach(_ -= subscriber)
      subscribers.remove(entityId, trieSetEmpty)
    }
  }
}
import AsyncDatahub._

class AsyncDatahub()(implicit val ec: ExecutionContext) extends Datahub[Future] {
  protected val innerStorage = new MemoryInnerStorage
  protected val log = LoggerFactory.getLogger(getClass)

  override def register(facade: EntityFacade): Unit =
    innerStorage.registerFacade(facade)

  def dataUpdated(entity: Entity)(data: entity.ops.D): Unit =
    innerStorage.getSubscribers(entity.id).foreach(sendChangeToOne(entity, _)(data))

  def syncRelationClocks(subscriber: Subscriber, relationClocks: Map[Entity, Any]): Unit =
    relationClocks.foreach { case (relation, clock) =>
      innerStorage.facade(relation.id).foreach(syncData(_, subscriber, Some(clock)))
    }

  def sendChangeToOne(entity: Entity, subscriber: Subscriber)
                     (entityData: entity.ops.D): Unit =
    subscriber.onUpdate(entity.id, entityData)

  def subscribe(entity: Entity,
                subscriber: Subscriber,
                lastKnownDataClock: Any): Boolean = {
    log.debug("subscribe {}, {}, {}", subscriber, entity.id, innerStorage.facade(entity.id))

    innerStorage.addSubscriber(entity.id, subscriber)

    innerStorage.facade(entity.id).map(syncData(_, subscriber, Some(lastKnownDataClock))).isDefined
  }

  protected def syncData(entityFacade: EntityFacade,
                         subscriber: Subscriber,
                         lastKnownDataClockOpt: Option[Any]): Future[Unit] = {
    log.debug(
      "Subscribe on {} with last known relation clock {}",
      Seq(entityFacade.entity.id, lastKnownDataClockOpt)
    )

    val entityOps: entityFacade.entity.ops.type = entityFacade.entity.ops

    val lastKnownDataClock = lastKnownDataClockOpt.flatMap(entityOps.matchClock) getOrElse entityOps.zero.clock

    entityFacade.getUpdatesFrom(lastKnownDataClock).map(d =>
      sendChangeToOne(entityFacade.entity, subscriber)(d)
    )
  }

  // TODO: add test
  def unsubscribe(entity: Entity, subscriber: Subscriber): Unit = {
    log.debug("Unsubscribe from relation {}", entity.id)

    innerStorage.removeSubscriber(entity.id, subscriber)
  }
}
