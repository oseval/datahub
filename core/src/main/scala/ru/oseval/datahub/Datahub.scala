package ru.oseval.datahub

import org.slf4j.LoggerFactory
import ru.oseval.datahub.data.{Data, DataOps}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

object Datahub {
  trait Storage {
    def register(entityId: String, dataClock: Any): Future[Unit]
    def change(entityId: String, dataClock: Any): Future[Unit]
    def getLastClock(entityId: String): Future[Option[Any]]
  }

  private[datahub] sealed trait DatahubMessage
  private[datahub] abstract class Register[F <: EntityFacade](val dataFacade: F, val relationClocks: Map[String, Any]) extends DatahubMessage {
    val lastClock: dataFacade.entity.ops.D#C
    override def equals(obj: Any): Boolean = obj match {
      case o: Register[_] => dataFacade == o.dataFacade && relationClocks == o.relationClocks && lastClock == o.lastClock
      case _ => super.equals(obj)
    }
  }
  object Register {
    def apply(dataFacade: EntityFacade, relationClocks: Map[String, Any])
             (_lastClock: dataFacade.entity.ops.D#C): Register[dataFacade.type] = {
      new Register[dataFacade.type](dataFacade, relationClocks) {
        override val lastClock: dataFacade.entity.ops.D#C = _lastClock
      }
    }
    def unapply[F <: EntityFacade](register: Register[F]): Option[(register.dataFacade.type, register.dataFacade.entity.ops.D#C, Map[String, Any])] =
      Some((register.dataFacade, register.lastClock, register.relationClocks))
  }
  // TODO: add added and removed relations with their clocks?
  private[datahub] case class DataUpdated(entityId: String, data: Data) extends DatahubMessage
  private[datahub] case class SyncRelationClocks(entityId: String, relationClocks: Map[String, Any])
    extends DatahubMessage
}

import Datahub._

abstract class Datahub(_storage: Storage, implicit val ec: ExecutionContext) {
  private val storage = new MemoryFallbackStorage(_storage)(ec)
  private val log = LoggerFactory.getLogger(getClass)
  private implicit val timeout: FiniteDuration = 3.seconds

  private val facades = mutable.Map.empty[String, EntityFacade]
  private val subscriptions = mutable.Map.empty[String, Set[String]] // facade -> subscriptions
  private val reverseSubscriptions = mutable.Map.empty[String, Set[String]] // facade -> related

  def receive(msg: DatahubMessage): Future[Unit] = msg match {
    // TODO: Ask relations if has restricted subscription rights (from inside Facade)
    case Register(facade, lastClock, relationClocks) =>
      facades += (facade.entity.id → facade)

      // this facade depends on that relations
      relationClocks.foreach { case (id, clock) => subscribe(facade, id, Some(clock)) }

      // sync registered entity clock
      storage.getLastClock(facade.entity.id).flatMap { lastStoredClockOpt =>
        val fops: facade.entity.ops.type = facade.entity.ops

        lastStoredClockOpt.flatMap(fops.matchClock).map(lastStoredClock =>
          if (fops.ordering.gt(lastClock, lastStoredClock))
            facade.getUpdatesFrom(lastStoredClock).flatMap(d => receive(DataUpdated(facade.entity.id, d)))
          else Future.unit
        ).getOrElse(Future.unit)
      }.flatMap(_ => storage.register(facade.entity.id, lastClock))

    case DataUpdated(entityId, _data) =>
      facades.get(entityId).fold(
        Future.failed[Unit](new Exception("Facade with id=" + entityId + " is not registered"))
      ) { facade =>
        facade.entity.ops.matchData(_data).fold(
          Future.failed[Unit](new Exception(
            "Entity " + entityId + " with taken facade " +
              facade.entity.id + " does not match data " + _data.getClass.getName
          ))
        ) { data =>
          // who subscribed on that facade
          subscriptions
            .getOrElse(facade.entity.id, Set.empty)
            .flatMap(facades.get)
            .foreach(f => sendChangeToOne(f, facade.entity)(data))
          // the facades on which that facade depends
          val relatedFacades = facade.entity.ops.getRelations(data)
          val removed = reverseSubscriptions.getOrElse(entityId, Set.empty) -- relatedFacades
          val added = relatedFacades -- reverseSubscriptions.getOrElse(facade.entity.id, Set.empty)

          removed.foreach(unsubscribe(facade, _))
          added.foreach(subscribe(facade, _, None))

          storage.change(entityId, data.clock)
        }
      }

    case SyncRelationClocks(entityId, relationClocks) =>
      facades.get(entityId).foreach { f =>
        relationClocks.foreach { case (rid, clock) => syncRelation(f, rid, Some(clock)) }
      }

      Future.unit
  }

  protected def sendChangeToOne(to: EntityFacade, related: Entity)(relatedData: related.ops.D): Future[Unit] =
    to.onUpdate(related.id, relatedData)

  private def syncRelation(facade: EntityFacade, relatedId: String, lastKnownDataClockOpt: Option[Any]): Unit = {
    facades.get(relatedId).foreach { related =>
      log.debug("Subscribe entity {} on {} with last known related clock {}", facade.entity.id, relatedId, lastKnownDataClockOpt)

      val relops: related.entity.ops.type = related.entity.ops

      storage.getLastClock(relatedId).foreach(_.flatMap(relops.matchClock).foreach { lastClock =>

        val lastKnownDataClock = lastKnownDataClockOpt.flatMap(relops.matchClock) getOrElse relops.zero.clock

        log.debug("lastClock {}, lastKnownClock {}, {}",
          Seq(lastClock, lastKnownDataClock, relops.ordering.gt(lastClock, lastKnownDataClock))
        )

        if (relops.ordering.gt(lastClock, lastKnownDataClock))
          related.getUpdatesFrom(lastKnownDataClock).foreach(d =>
            sendChangeToOne(facade, related.entity)(d)
          )
      })
    }
  }

  private def subscribe(facade: EntityFacade, relatedId: String, lastKnownDataClockOpt: Option[Any]): Unit = {
    log.debug("subscribe {}, {}, {}", facade.entity.id, relatedId, facades.get(relatedId))
    val relatedSubscriptions = subscriptions.getOrElse(relatedId, Set.empty)

    facades.get(relatedId).foreach(relation =>
      if (relation.entity.untrustedKinds contains facade.entity.kind) {
        // TODO: request facade to get approve on subscription
        log.warn("Failed to subscribe on {} due untrusted kind {}{}", relatedId, facade.entity.kind, "")
      } else {
        subscriptions.update(relatedId, relatedSubscriptions + facade.entity.id)

        reverseSubscriptions.update(
          facade.entity.id,
          reverseSubscriptions.getOrElse(facade.entity.id, Set.empty) + relatedId
        )

        syncRelation(facade, relatedId, lastKnownDataClockOpt)
      }
    )
  }

  // TODO: add test
  private def unsubscribe(facade: EntityFacade, relatedId: String): Unit = {
    log.debug("Unsubscribe entity {} from related {}", Seq(facade.entity.id, relatedId): _*)
    val newRelatedSubscriptions = subscriptions.getOrElse(facade.entity.id, Set.empty) - facade.entity.id
    if (newRelatedSubscriptions.isEmpty) subscriptions -= relatedId
    else subscriptions.update(relatedId, newRelatedSubscriptions)
  }
}

class MemoryStorage extends Datahub.Storage {
  private val ids = mutable.Map.empty[String, Any]

  override def register(entityId: String, dataClock: Any): Future[Unit] =
    Future.successful {
      ids.update(entityId, dataClock)
    }

  override def change(entityId: String, dataClock: Any): Future[Unit] =
    Future.successful {
      ids.update(entityId, dataClock)
    }

  def getLastClock(entityId: String): Future[Option[Any]] = {
    Future.successful(ids.get(entityId))
  }
}

class MemoryFallbackStorage(storage: Storage)(implicit ec: ExecutionContext) extends MemoryStorage {
  override def register(entityId: String, dataClock: Any): Future[Unit] =
    super.register(entityId, dataClock).flatMap(_ => storage.register(entityId, dataClock))

  override def change(entityId: String, dataClock: Any): Future[Unit] =
    super.change(entityId, dataClock).flatMap(_ => storage.change(entityId, dataClock))

  override def getLastClock(entityId: String): Future[Option[Any]] =
    storage.getLastClock(entityId).flatMap {
      case None => super.getLastClock(entityId)
      case opt => Future.successful(opt)
    }
}