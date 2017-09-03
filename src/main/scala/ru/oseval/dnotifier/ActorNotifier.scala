package ru.oseval.dnotifier

import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern.pipe
import akka.stream.scaladsl.{Sink, Source}
import akka.stream._

import scala.concurrent.Future
import scala.concurrent.duration._

object ActorNotifier {
  private[dnotifier] sealed trait NotifierMessage
  private[dnotifier] case class Register(dataFacade: EntityFacade,
                                         lastClock: String,
                                         relationClocks: Map[String, String]) extends NotifierMessage
  private[dnotifier] case class NotifyDataUpdated(entityId: String, data: Data) extends NotifierMessage

  def props(storage: Notifier.Storage): Props = Props(classOf[ActorNotifier], storage)
}
import ActorNotifier._


class ActorNotifier(storage: Notifier.Storage) extends Actor with ActorLogging {
  import context.dispatcher
  private implicit val timeout: FiniteDuration = 3.seconds

  private lazy val system = context.system
  private implicit lazy val mat = ActorMaterializer(
    ActorMaterializerSettings(system).withSupervisionStrategy(Supervision.resumingDecider: Supervision.Decider),
    "SubscriptionQueueMaterializer"
  )(system)

  case class DataTask(facade: EntityFacade, relatedId: String, relatedData: Data)
  private val subscriptionQueue = Source
    .queue[DataTask](1000, OverflowStrategy.dropHead)
    .mapAsyncUnordered(100) { case DataTask(facade, relatedId, relatedData) ⇒
      facade.onUpdate(relatedId, relatedData).recover { case e =>
        log.error(e, "Failed to send about update {} of related {} to facade {}", relatedData.clock, relatedId, facade.entity.id)
      }
    }
    .to(Sink.ignore)
    .run

  private val notifier = new Notifier(storage, context.dispatcher) {
    override protected def sendChangeToOne(to: EntityFacade, related: Entity)
                                          (relatedData: related.D): Future[Unit] =
      queueOffer(DataTask(to, related.id, relatedData))
  }

  override def receive: Receive = {
    case Register(facade, lastClock, relationClocks) =>
      notifier.register(facade, lastClock, relationClocks) pipeTo sender()

    case NotifyDataUpdated(entityId, data) ⇒
      notifier.dataUpdated(entityId, data) pipeTo sender()
  }

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
}
