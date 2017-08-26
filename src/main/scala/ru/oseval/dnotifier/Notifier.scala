package ru.oseval.dnotifier

import akka.actor.{Actor, ActorLogging, Props, Status}
import akka.pattern.pipe
import akka.stream._
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

object Notifier {
  trait Storage {
    def register(entityId: String, dataClock: String): Future[Unit]
    def change(entityId: String, dataClock: String): Future[Unit]
    def getLastId(entityId: String): Future[Option[String]]
  }

  private[dnotifier] sealed trait NotifierMessage
  private[dnotifier] case class Register(dataFacade: EntityFacade,
                                                    lastClock: String,
                                                    relationClocks: Map[String, String]) extends NotifierMessage
  // TODO: add added and removed relations with their clocks?
  private[dnotifier] case class NotifyDataUpdated(entityId: String, data: Data) extends NotifierMessage

  def props(storage: Storage): Props = Props(classOf[Notifier], storage)
}

import Notifier._

private class Notifier(storage: Storage) extends Actor with ActorLogging {
  import context.dispatcher
  private implicit val timeout: Timeout = 3.seconds

  private val facades = mutable.Map.empty[String, EntityFacade]
  private val subscriptions = mutable.Map.empty[String, Set[String]] // facade -> subscriptions
  private val reverseSubscriptions = mutable.Map.empty[String, Set[String]] // facade -> related

  private lazy val system = context.system
  private implicit lazy val mat = ActorMaterializer(
    ActorMaterializerSettings(system).withSupervisionStrategy(Supervision.resumingDecider: Supervision.Decider),
    "SubscriptionQueueMaterializer"
  )(system)

  private val subscriptionQueue = Source
    .queue[DataTask](1000, OverflowStrategy.dropHead)
    .mapAsyncUnordered(100) { case DataTask(facade, relatedId, relatedData) ⇒
      facade.onUpdate(relatedId, relatedData).recover { case e =>
        log.error(e, "Failed to send about update {} of related {} to facade {}", relatedData.clock, relatedId, facade.entity.id)
      }
    }
    .to(Sink.ignore)
    .run

  class DataTask private[DataTask] (val facade: EntityFacade, val related: Entity, val relatedData: Any)
  object DataTask {
    def apply(facade: EntityFacade, related: Entity)(relatedData: related.D): DataTask =
      new DataTask(facade, related, relatedData)
    def unapply(arg: DataTask): Option[(arg.facade.type, String, arg.facade.entity.D)] =
      Some((arg.facade, arg.related.id, arg.relatedData.asInstanceOf[arg.facade.entity.D]))
  }

  override def receive: Receive = {
    case Register(facade, lastClock, relationClocks) ⇒
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
      } pipeTo sender()

    case NotifyDataUpdated(entityId, data) ⇒
      facades.get(entityId) match {
        case Some(facade) =>
          // who subscribed on that facade
          subscriptions
            .getOrElse(facade.entity.id, Set.empty)
            .flatMap(facades.get)
            .foreach(f => sendChangeToOne(f, facade.entity)(facade.entity.matchData(data).get))

          // the facades on which that facade depends
          val relatedFacades = facade.entity.ops.getRelations(facade.entity.matchData(data).get)
          val removed = reverseSubscriptions.getOrElse(entityId, Set.empty) -- relatedFacades
          val added = relatedFacades -- reverseSubscriptions.getOrElse(facade.entity.id, Set.empty)

          removed.foreach(unsubscribe(facade, _))
          added.foreach(subscribe(facade, _, None))

          storage.change(entityId, data.clock) pipeTo sender()

        case None ⇒
          sender() ! Status.Failure(new Exception("Facade with id=" + entityId + " is not registered"))
      }

//    case SyncWithRelated(entityId, relatedId, relatedClock) ⇒

  }

  private def sendChangeToOne(to: EntityFacade, related: Entity)(relatedData: related.D): Future[Unit] =
    queueOffer(DataTask(to, related)(relatedData))

  protected def queueOffer(task: DataTask): Future[Unit] = {
    subscriptionQueue.offer(task).map {
      case QueueOfferResult.Enqueued ⇒ ()
      case QueueOfferResult.Failure(e) ⇒
        log.error(e, "Queueing task {} failed", task)
        throw e
      case other ⇒
        log.error("Queueing task {} failed with result {}", task, other)
        throw new Exception("Queue offer for task " + task + " failed with status " + other.toString)
    }
  }

  private def subscribe(facade: EntityFacade, relatedId: String, lastKnownDataClockOpt: Option[String]): Unit = {
    log.debug("subscribe {}, {}, {}", facade.entity.id, relatedId, facades.get(relatedId))
    val relatedSubscriptions = subscriptions.getOrElse(relatedId, Set.empty)
    subscriptions.update(relatedId, relatedSubscriptions + facade.entity.id)
    reverseSubscriptions.update(facade.entity.id, reverseSubscriptions.getOrElse(facade.entity.id, Set.empty) + relatedId)

    facades.get(relatedId).foreach { related =>
      log.debug("Subscribe entity {} on {} with last known data id {}", facade.entity.id, relatedId, lastKnownDataClockOpt)

      storage.getLastId(relatedId).foreach(_.foreach { lastClock =>
        println(("wefwfwefwf"))
        val lastKnownDataClock = lastKnownDataClockOpt getOrElse related.entity.ops.zero.clock

        log.debug("lastClock {}, lastKnownClock {}, {}", lastClock, lastKnownDataClock, related.entity.ops.ordering.gt(lastClock, lastKnownDataClock))

        if (related.entity.ops.ordering.gt(lastClock, lastKnownDataClock))
          related.getUpdatesFrom(lastKnownDataClock).foreach(d =>
            sendChangeToOne(facade, related.entity)(related.entity.matchData(d).get)
          )
      })
    }
  }

  private def unsubscribe(facade: EntityFacade, relatedId: String): Unit = {
    log.debug("Unsubscribe entity {} from related {}", facade.entity.id, relatedId)
    val newRelatedSubscriptions = subscriptions.getOrElse(facade.entity.id, Set.empty) - facade.entity.id
    if (newRelatedSubscriptions.isEmpty) subscriptions -= relatedId
    else subscriptions.update(relatedId, newRelatedSubscriptions)
  }
}

class MemoryStorage extends Notifier.Storage {
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