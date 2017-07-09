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
    def register(entityId: String, dataId: String): Future[Unit]
    def change(entityId: String, dataId: String): Future[Unit]
    def getLastId(entityId: String): Future[String]
  }

  private[dnotifier] sealed trait NotifierMessage
  private[dnotifier] case class Register(dataFacade: EntityFacade,
                                         relatedFacades: Map[String, String] = Map.empty) extends NotifierMessage
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
        log.error(e, "Failed to send about update {} of related {} to facade {}", relatedData.clock, relatedId, facade.id)
      }
    }
    .to(Sink.ignore)
    .run

  case class DataTask(facade: EntityFacade, relatedId: String, relatedData: Data)

  override def receive: Receive = {
    case Register(facade, relatedFacades) ⇒
      // if facade is not registered then the facades subscribed to them
      // need to be notified about last data if id bigger then their own
      facades += (facade.id → facade)

      storage.register(facade.id, facade.ops.zero.clock).map { _ ⇒
        reverseSubscriptions.update(
          facade.id,
          reverseSubscriptions.getOrElse(facade.id, Set.empty) ++ relatedFacades.keySet
        )

        relatedFacades.foreach { case (id, clock) => subscribe(facade, id, Some(clock)) }
      } pipeTo sender()

    case NotifyDataUpdated(entityId, data) ⇒
      facades.get(entityId) match {
        case Some(facade) ⇒
          // who subscribed on that facade
          subscriptions
            .getOrElse(facade.id, Set.empty)
            .flatMap(facades.get)
            .foreach(sendChangeToOne(_, entityId, data))

          // the facades on which that facade depends
          val relatedFacades = facade.ops.getRelatedEntities(data.asInstanceOf[facade.ops.DInner])
          val removed = reverseSubscriptions.getOrElse(entityId, Set.empty) -- relatedFacades
          val added = relatedFacades -- reverseSubscriptions.getOrElse(facade.id, Set.empty)

          removed.foreach(unsubscribe(facade, _))
          added.foreach(subscribe(facade, _, None))

          storage.change(entityId, data.clock) pipeTo sender()

        case None ⇒
          sender() ! Status.Failure(new Exception("Facade with id=" + entityId + " is not registered"))
      }
  }

  private def sendChangeToOne(to: EntityFacade, relatedId: String, relatedData: Data): Future[Unit] =
    queueOffer(DataTask(to, relatedId, relatedData))

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
    val relatedSubscriptions = subscriptions.getOrElse(relatedId, Set.empty)
    subscriptions.update(relatedId, relatedSubscriptions + facade.id)
    reverseSubscriptions.update(facade.id, reverseSubscriptions.getOrElse(facade.id, Set.empty) + relatedId)

    facades.get(relatedId).map { related =>
      val lastKnownDataClock = lastKnownDataClockOpt.getOrElse(related.ops.zero.clock)
      log.debug("Subscribe entity {} on {} with last known data id {}", facade.id, relatedId, lastKnownDataClock)

      storage.getLastId(relatedId).map { lastId =>
        if (!related.ops.ordering.equiv(lastId, lastKnownDataClock))
          related
            .getUpdatesFrom(lastKnownDataClock)
            .map(sendChangeToOne(facade, relatedId, _))
      }
    }
  }

  private def unsubscribe(facade: EntityFacade, relatedId: String): Unit = {
    log.debug("Unsubscribe entity {} from related {}", facade.id, relatedId)
    val newRelatedSubscriptions = subscriptions.getOrElse(facade.id, Set.empty) - facade.id
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

  def getLastId(entityId: String): Future[String] = {
    Future.successful(ids(entityId))
  }
}