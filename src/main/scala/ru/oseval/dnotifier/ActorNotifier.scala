package ru.oseval.dnotifier

import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern.pipe
import akka.stream.scaladsl.{Sink, Source}
import akka.stream._
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

object ActorNotifier {
  private[dnotifier] sealed trait NotifierMessage
  private[dnotifier] case class Register(dataFacade: EntityFacade,
                                         lastClock: String,
                                         relationClocks: Map[String, String]) extends NotifierMessage
  // TODO: add added and removed relations with their clocks?
  private[dnotifier] case class NotifyDataUpdated(entityId: String, data: Data) extends NotifierMessage

  def props(notifier: Notifier): Props = Props(classOf[ActorNotifier], notifier)
}
import ActorNotifier._


class ActorNotifier(notifier: Notifier) extends Actor with ActorLogging {
  import context.dispatcher
  private implicit val timeout: Timeout = 3.seconds

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

  case class DataTask(facade: EntityFacade, relatedId: String, relatedData: Data)

  override def receive: Receive = {
    case Register(facade, lastClock, relationClocks) =>
      notifier.register(facade, lastClock, relationClocks) pipeTo sender()

    case NotifyDataUpdated(entityId, data) ⇒
      notifier.dataUpdated(entityId, data) pipeTo sender()
  }

  override protected def sendChangeToOne(to: EntityFacade, related: Entity)
                                        (relatedData: related.D): Future[Unit] =
    queueOffer(DataTask(to, related.id, relatedData))

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
        val lastKnownDataClock = lastKnownDataClockOpt getOrElse related.entity.ops.zero.clock

        log.debug("lastClock {}, lastKnownClock {}, {}", lastClock, lastKnownDataClock, related.entity.ops.ordering.gt(lastClock, lastKnownDataClock))

        if (related.entity.ops.ordering.gt(lastClock, lastKnownDataClock))
          related.getUpdatesFrom(lastKnownDataClock).foreach(d =>
            sendChangeToOne(facade, related.entity)(d)
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
