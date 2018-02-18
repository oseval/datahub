package ru.oseval.datahub

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.stream._
import akka.stream.scaladsl.{Sink, Source}
import ru.oseval.datahub.data.Data

import scala.concurrent.Future
import scala.concurrent.duration._

object ActorDatahub {
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
//  private[Datahub] case class SubscribeApproved(facade: EntityFacade,
//                                                relatedId: String,
//                                                lastKnownDataClockOpt: Option[Any]) extends DatahubMessage

  def props(storage: Datahub.Storage): Props = Props(classOf[ActorDatahubImpl], storage)
}
import ActorDatahub._

case class ActorDatahub(ref: ActorRef)(implicit timeout: FiniteDuration) extends Datahub[Future] {
  override def register(facade: EntityFacade)
                       (lastClock: facade.entity.ops.D#C, relationClocks: Map[String, Any]): Future[Unit] =
    (ref ? Register(facade, relationClocks)(lastClock)).map(_ => ())

  // TODO: actor just for akka.Replicator. Wrap regular datahub to send replication data
  override def setForcedSubscribers(entityId: String, forced: Set[EntityFacade]): Future[Unit] = ???

  override def dataUpdated(entityId: String, _data: Data): Future[Unit] = ???

  override def syncRelationClocks(entityId: String, relationClocks: Map[String, Any]): Future[Unit] = ???
}

private class ActorDatahubImpl(storage: Datahub.Storage) extends Actor with ActorLogging {
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

  private val datahub = new AsyncDatahub(storage)(context.dispatcher) {
    override protected def sendChangeToOne(to: EntityFacade, relation: Entity)
                                          (relationData: relation.ops.D): Future[Unit] =
      queueOffer(DataTask(to, relation.id, relationData))
  }

  override def receive: Receive = {
    case Register(facade, lastClock, relationClocks) =>
      datahub.register(facade)(lastClock, relationClocks) pipeTo sender()
    case DataUpdated(entityId, data) => datahub.dataUpdated(entityId, data)
    case SyncRelationClocks(entityId, relationClocks) => datahub.syncRelationClocks(entityId, relationClocks)
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
