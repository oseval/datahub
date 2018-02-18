package ru.oseval.datahub

import org.slf4j.LoggerFactory
import ru.oseval.datahub.data.Data

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

object Datahub {
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
}

import Datahub._

trait Datahub[M[_]] {
  def register(facade: EntityFacade)(lastClock: facade.entity.ops.D#C, relationClocks: Map[String, Any]): M[Unit]
  def setForcedSubscribers(entityId: String, forced: Set[EntityFacade]): M[Unit]
  def dataUpdated(entityId: String, _data: Data): M[Unit]
  def syncRelationClocks(entityId: String, relationClocks: Map[String, Any]): M[Unit]
}

class AsyncDatahub(_storage: Storage)(implicit val ec: ExecutionContext) extends Datahub[Future] {
  private val storage = new MemoryFallbackStorage(_storage)(ec)
  private val log = LoggerFactory.getLogger(getClass)
  private implicit val timeout: FiniteDuration = 3.seconds

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

  def register(facade: EntityFacade)
              (lastClock: facade.entity.ops.D#C, relationClocks: Map[String, Any]): Future[Unit] = {
    facades.put(facade.entity.id, facade)

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
    Future.successful(forcedSubscribers.update(entityId, forced))

  def dataUpdated(entityId: String, _data: Data): Future[Unit] = {
    facades.get(entityId).fold(
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
          subscribers.get(facade.entity.id).foreach(_
            .keySet.flatMap(facades.get)
            .foreach(f => sendChangeToOne(f, facade.entity)(data))
          )
          // the facades on which that facade depends
          val relatedFacades = facade.entity.ops.getRelations(data)
          val oldRelations = relations.get(entityId).map(_.keySet) getOrElse Set.empty[String]
          (oldRelations -- relatedFacades).foreach(unsubscribe(facade, _))
          (relatedFacades -- oldRelations).foreach(subscribe(facade, _, None))
        }
      }
    }
  }

  def syncRelationClocks(entityId: String, relationClocks: Map[String, Any]): Future[Unit] = {
    facades.get(entityId).foreach { f =>
      relationClocks.foreach { case (rid, clock) => syncRelation(f, rid, Some(clock)) }
    }

    Future.unit
  }

  private def subscribeApproved(facade: EntityFacade, relationId: String, lastKnownDataClockOpt: Option[Any]): Future[Unit] = {
    subscribers.putIfAbsent(relationId, trieSetEmpty)
    subscribers(relationId).update(facade.entity.id, true)

    relations.putIfAbsent(facade.entity.id, trieSetEmpty)
    relations(facade.entity.id).update(relationId, true)

    syncRelation(facade, relationId, lastKnownDataClockOpt)

    Future.unit
  }

  protected def sendChangeToOne(to: EntityFacade, relation: Entity)(relationData: relation.ops.D): Future[Unit] =
    to.onUpdate(relation.id, relationData)

  private def syncRelation(facade: EntityFacade, relationId: String, lastKnownDataClockOpt: Option[Any]): Unit = {
    facades.get(relationId).foreach { relation =>
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
    log.debug("subscribe {}, {}, {}", facade.entity.id, relatedId, facades.get(relatedId))

    facades.get(relatedId).foreach(relation =>
      relation.requestForApprove(facade.entity).map(
        if (_) subscribeApproved(facade, relatedId, lastKnownDataClockOpt)
        else log.warn("Failed to subscribe on {} due untrusted kind {}{}", relatedId, facade.entity.ops.kind, "")
      )
    )
  }

  // TODO: add test
  private def unsubscribe(facade: EntityFacade, relationId: String): Unit = {
    log.debug("Unsubscribe entity {} from related {}", Seq(facade.entity.id, relationId): _*)
    
    subscribers.get(relationId).foreach(_ -= facade.entity.id)
    subscribers.remove(relationId, trieSetEmpty)

    relations.get(facade.entity.id).foreach(_ -= relationId)
    relations.remove(facade.entity.id, trieSetEmpty)
  }
}