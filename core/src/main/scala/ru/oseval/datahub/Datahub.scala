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
  private[datahub] case class SyncRelationClock[C](relationId: String, clock: C)
}

import Datahub._

abstract class Datahub(storage: Storage, implicit val ex: ExecutionContext) {
  private val log = LoggerFactory.getLogger(getClass)
  private implicit val timeout: FiniteDuration = 3.seconds

  private val facades = mutable.Map.empty[String, EntityFacade]
  private val subscriptions = mutable.Map.empty[String, Set[String]] // facade -> subscriptions
  private val reverseSubscriptions = mutable.Map.empty[String, Set[String]] // facade -> related

  def receive(msg: DatahubMessage): Future[Unit] = msg match {
    case Register(facade, lastClock, relationClocks) =>
      // if facade is not registered then the facades subscribed to them
      // need to be notified about last data if id bigger then their own
      facades += (facade.entity.id → facade)

      reverseSubscriptions.update(
        facade.entity.id,
        reverseSubscriptions.getOrElse(facade.entity.id, Set.empty) ++ relationClocks.keySet
      )

      relationClocks.foreach { case (id, clock) => subscribe(facade, id, Some(clock)) }

      storage.getLastClock(facade.entity.id).flatMap { lastStoredClockOpt =>
        val fops: facade.entity.ops.type = facade.entity.ops
//        val lastStoredClock =
//          lastStoredClockOpt.flatMap(fops.matchClock) getOrElse fops.zero.clock
        lastStoredClockOpt.flatMap(fops.matchClock).foreach(lastStoredClock =>
          if (fops.ordering.gt(lastClock, lastStoredClock))
          // TODO: requiredUpatesFrom? new method to notify all subscribers about update
            facade.getUpdatesFrom(lastStoredClock)
        )
        storage.register(facade.entity.id, lastClock)
      }

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
  }

  protected def sendChangeToOne(to: EntityFacade, related: Entity)(relatedData: related.ops.D): Future[Unit] =
    to.onUpdate(related.id, relatedData)

  private def subscribe(facade: EntityFacade, relatedId: String, lastKnownDataClockOpt: Option[Any]): Unit = {
    log.debug("subscribe {}, {}, {}", facade.entity.id, relatedId, facades.get(relatedId))
    val relatedSubscriptions = subscriptions.getOrElse(relatedId, Set.empty)
    subscriptions.update(relatedId, relatedSubscriptions + facade.entity.id)
    reverseSubscriptions.update(facade.entity.id, reverseSubscriptions.getOrElse(facade.entity.id, Set.empty) + relatedId)

    facades.get(relatedId).foreach { related =>
      log.debug("Subscribe entity {} on {} with last known related clock {}", facade.entity.id, relatedId, lastKnownDataClockOpt)

      val relops: related.entity.ops.type = related.entity.ops

      // TODO: since related was register we need to send updates to subscriber from last clock
      // TODO: even it is not exists in storage - fallback to Datahub state
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