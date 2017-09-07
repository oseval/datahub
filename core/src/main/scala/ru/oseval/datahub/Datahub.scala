package ru.oseval.datahub

import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

object Datahub {
  trait Storage {
    def register(entityId: String, dataClock: String): Future[Unit]
    def change(entityId: String, dataClock: String): Future[Unit]
    def getLastId(entityId: String): Future[Option[String]]
  }

  private[datahub] sealed trait DatahubMessage
  private[datahub] case class Register(dataFacade: EntityFacade,
                                       lastClock: String,
                                       relationClocks: Map[String, String]) extends DatahubMessage
  // TODO: add added and removed relations with their clocks?
  private[datahub] case class DataUpdated(entityId: String, data: Data) extends DatahubMessage
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

      storage.getLastId(facade.entity.id).flatMap { lastStoredClockOpt =>
        lastStoredClockOpt.foreach(lastStoredClock =>
          if (facade.entity.ops.ordering.gt(lastClock, lastStoredClock))
          // TODO: requiredUpatesFrom? new method to notify all subscribers about update
            facade.getUpdatesFrom(lastStoredClock)
        )
        storage.register(facade.entity.id, lastClock)
      }

    case DataUpdated(entityId, _data) =>
      facades.get(entityId).fold(
        Future.failed[Unit](new Exception("Facade with id=" + entityId + " is not registered"))
      ) { facade =>
        facade.entity.matchData(_data).fold(
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

  protected def sendChangeToOne(to: EntityFacade, related: Entity)(relatedData: related.D): Future[Unit] =
    to.onUpdate(related.id, relatedData)

  private def subscribe(facade: EntityFacade, relatedId: String, lastKnownDataClockOpt: Option[String]): Unit = {
    log.debug("subscribe {}, {}, {}", facade.entity.id, relatedId, facades.get(relatedId))
    val relatedSubscriptions = subscriptions.getOrElse(relatedId, Set.empty)
    subscriptions.update(relatedId, relatedSubscriptions + facade.entity.id)
    reverseSubscriptions.update(facade.entity.id, reverseSubscriptions.getOrElse(facade.entity.id, Set.empty) + relatedId)

    facades.get(relatedId).foreach { related =>
      log.debug("Subscribe entity {} on {} with last known data id {}", facade.entity.id, relatedId, lastKnownDataClockOpt)

      storage.getLastId(relatedId).foreach(_.foreach { lastClock =>
        val lastKnownDataClock = lastKnownDataClockOpt getOrElse related.entity.ops.zero.clock

        log.debug("lastClock {}, lastKnownClock {}, {}",
          Seq(lastClock, lastKnownDataClock, related.entity.ops.ordering.gt(lastClock, lastKnownDataClock))
        )

        if (related.entity.ops.ordering.gt(lastClock, lastKnownDataClock))
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
  private val ids = mutable.Map.empty[String, String]

  override def register(entityId: String, dataId: String): Future[Unit] =
    Future.successful {
      ids.update(entityId, dataId)
    }

  override def change(entityId: String, dataId: String): Future[Unit] =
    Future.successful {
      ids.update(entityId, dataId)
    }

  def getLastId(entityId: String): Future[Option[String]] = {
    Future.successful(ids.get(entityId))
  }
}