package ru.oseval.datahub

import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern.pipe
import akka.stream._
import akka.stream.scaladsl.{Sink, Source}
import ru.oseval.datahub.Datahub.DatahubMessage

import scala.concurrent.Future
import scala.concurrent.duration._

object ActorDatahub {
  def props(storage: Datahub.Storage): Props = Props(classOf[ActorDatahub], storage)
}


class ActorDatahub(storage: Datahub.Storage) extends Actor with ActorLogging {
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

  private val datahub = new Datahub(storage, context.dispatcher) {
    override protected def sendChangeToOne(to: EntityFacade, related: Entity)
                                          (relatedData: related.D): Future[Unit] =
      queueOffer(DataTask(to, related.id, relatedData))
  }

  override def receive: Receive = {
    case msg: DatahubMessage => datahub.receive(msg) pipeTo sender()
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
