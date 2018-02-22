package ru.oseval.datahub

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.pattern.{ask, pipe}
import akka.stream._
import akka.stream.scaladsl.{Sink, Source}
import ru.oseval.datahub
import ru.oseval.datahub.AsyncDatahub.Storage
import ru.oseval.datahub.data.Data

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

object ActorDatahub {
  class InnerStorageDecorator(system: ActorSystem) extends AsyncDatahub.InnerDataStorage {
    private def receive(cmd: CMD): Unit = cmd match {
      case Facades =>
      case Relations =>
      case ForcedSubscribers =>
    }
    private lazy val replicator = system.actorOf(Props(classOf[ReplicatorBridge], receive _))

    override def facade(entityId: String): Option[EntityFacade] = ???

    override def registerFacade(entityFacade: EntityFacade): Unit = ???

    override def getSubscribers(entityId: String): Set[String] = ???

    override def getRelations(entityId: String): Set[String] = ???

    override def addRelation(entityId: String, subscriberId: String): Unit = ???

    override def removeRelation(entityId: String, subscriberId: String): Unit = ???

    override def setForcedSubscribers(entityId: String, subscribers: Set[EntityFacade]): Unit = ???
  }
}

case class ActorDatahub[M[_]](storage: Storage)
                             (implicit system: ActorSystem, ec: ExecutionContext)
  extends AsyncDatahub(storage, new datahub.ActorDatahub.InnerStorageDecorator(system))(ec)

private class ReplicatorBridge(storage: Datahub.Storage) extends Actor with ActorLogging {
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
