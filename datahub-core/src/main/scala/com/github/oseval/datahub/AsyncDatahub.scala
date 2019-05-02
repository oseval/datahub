package com.github.oseval.datahub

import com.github.oseval.datahub.data.Data
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

object AsyncDatahub {
  private[datahub] class MemoryInnerStorage {
    private val trieSetEmpty: TrieMap[Subscriber, Boolean] = TrieMap.empty[Subscriber, Boolean]
    private val sources: TrieMap[String, LocalDatasource] =
      TrieMap.empty[String, LocalDatasource]
    private val remoteSources: TrieMap[String, RemoteDatasource] =
      TrieMap.empty[String, RemoteDatasource]
    private val subscribers: TrieMap[String, TrieMap[Subscriber, Boolean]] =
      TrieMap.empty[String, TrieMap[Subscriber, Boolean]] // source -> subscribers

    def source(entity: Entity): Option[Datasource] = sources.get(entity.id).orElse(remoteSources.get(entity.ops.kind))
    def registerSource(entitySource: Datasource): Unit = entitySource match {
      case s: LocalDatasource => sources.put(s.entity.id, s)
      case s: RemoteDatasource => remoteSources.put(s.ops.kind, s)
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

  override def register(source: Datasource): Unit =
    innerStorage.registerSource(source)

  def dataUpdated(entity: Entity)(data: entity.ops.D): Unit =
    innerStorage.getSubscribers(entity.id).foreach(sendChangeToOne(entity, _)(data))

  def dataUpdated(entityId: String, data: Data): Unit =
    innerStorage.getSubscribers(entityId).foreach(sendChangeToOne(entityId, _)(data))

  def syncRelationClocks(subscriber: Subscriber, relationClocks: Map[Entity, Any]): Unit =
    relationClocks.foreach { case (relation, clock) =>
      innerStorage.source(relation).foreach(syncData(_, relation, subscriber, Some(clock)))
    }

  def sendChangeToOne(entity: Entity, subscriber: Subscriber)
                     (entityData: entity.ops.D): Unit =
    subscriber.onUpdate(entity)(entityData)

  def sendChangeToOne(entityId: String, subscriber: Subscriber)(entityData: Data): Unit =
    subscriber.onUpdate(entityId, entityData)

  def subscribe(entity: Entity,
                subscriber: Subscriber,
                lastKnownDataClock: Any): Boolean = {
    log.debug("subscribe {}, {}, {}", subscriber, entity.id, innerStorage.source(entity))

    innerStorage.addSubscriber(entity.id, subscriber)

    innerStorage.source(entity).map { s =>
      s match { case rem: RemoteDatasource => rem.onSubscribe(entity, subscriber, lastKnownDataClock) case _ => }
      syncData(s, entity, subscriber, Some(lastKnownDataClock))
    }.isDefined
  }

  protected def syncData(entitySource: Datasource,
                         entity: Entity,
                         subscriber: Subscriber,
                         lastKnownDataClockOpt: Option[Any]): Unit = {
    log.debug(
      "Subscribe on {} with last known relation clock {}",
      Seq(entity.id, lastKnownDataClockOpt)
    )

    // TODO: optimize this on datasource level (e.g. store last clock inside global datasource entity
    entitySource match {
      case f: LocalDatasource =>
        val ops: f.entity.ops.type = f.entity.ops
        val lastKnownDataClock = lastKnownDataClockOpt.flatMap(ops.matchClock) getOrElse ops.zero.clock
        f.syncData(lastKnownDataClock)
      case f: RemoteDatasource =>
        val ops: f.ops.type = f.ops
        val lastKnownDataClock = lastKnownDataClockOpt.flatMap(f.ops.matchClock) getOrElse ops.zero.clock
        f.syncData(entity.id, lastKnownDataClock)
    }
  }

  // TODO: add test
  def unsubscribe(entity: Entity, subscriber: Subscriber): Unit = {
    log.debug("Unsubscribe from relation {}", entity.id)

    innerStorage.removeSubscriber(entity, subscriber)
  }
}
