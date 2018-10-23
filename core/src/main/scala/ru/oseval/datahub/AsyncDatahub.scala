package ru.oseval.datahub

import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

object AsyncDatahub {
  private[datahub] class MemoryInnerStorage {
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

class AsyncDatahub()(implicit val ec: ExecutionContext) extends Datahub[Future] {
  protected val innerStorage = new MemoryInnerStorage
  protected val log = LoggerFactory.getLogger(getClass)

  override def register(facade: EntityFacade): Unit =
    innerStorage.registerFacade(facade)

  def dataUpdated(entity: Entity)(data: entity.ops.D): Unit =
    innerStorage.getSubscribers(entity.id).foreach(sendChangeToOne(entity, _)(data))

  def syncRelationClocks(entity: Entity, relationClocks: Map[Entity, Any]): Unit =
    relationClocks.foreach { case (relation, clock) =>
      innerStorage.facade(relation.id).foreach(syncData(_, entity, Some(clock)))
    }

  def sendChangeToOne(entity: Entity, subscriber: Entity)
                     (entityData: entity.ops.D): Option[Future[Unit]] =
    innerStorage.facade(subscriber.id).map(_.onUpdate(entity.id, entityData))

  def subscribe(entity: Entity,
                subscriber: Entity,
                lastKnownDataClockOpt: Option[Any]): Boolean = {
    log.debug("subscribe {}, {}, {}", subscriber, entity.id, innerStorage.facade(entity.id))

    innerStorage.addSubscriber(entity.id, subscriber)

    innerStorage.facade(entity.id).map(syncData(_, subscriber, lastKnownDataClockOpt)).isDefined
  }

  protected def syncData(entityFacade: EntityFacade,
                         subscriber: Entity,
                         lastKnownDataClockOpt: Option[Any]): Future[Unit] = {
    log.debug(
      "Subscribe entity {} on {} with last known relation clock {}",
      subscriber.id, entityFacade.entity.id, lastKnownDataClockOpt
    )

    val entityOps: entityFacade.entity.ops.type = entityFacade.entity.ops

    val lastKnownDataClock = lastKnownDataClockOpt.flatMap(entityOps.matchClock) getOrElse entityOps.zero.clock

    entityFacade.getUpdatesFrom(lastKnownDataClock).flatMap(d =>
      sendChangeToOne(entityFacade.entity, subscriber)(d) getOrElse Future.unit
    )
  }

  // TODO: add test
  def unsubscribe(entity: Entity, subscriber: Entity): Unit = {
    log.debug("Unsubscribe entity {} from relation {}{}", subscriber.id, entity.id, "")

    innerStorage.removeSubscriber(entity.id, subscriber)
  }
}
