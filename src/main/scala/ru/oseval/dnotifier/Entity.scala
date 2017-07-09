package ru.oseval.dnotifier

import ru.oseval.dnotifier.Notifier.NotifyDataUpdated

import scala.concurrent.Future
import akka.actor.{Actor, ActorRef}
import akka.util.Timeout
import akka.pattern.ask
import ru.oseval.dnotifier.Data.{GetDifferenceFrom, RelatedDataUpdated}

trait Entity {
  val id: String
  val ops: DataOps
}

abstract class AbstractEntity(initialData: Data) extends Entity {
  val notifyDataUpdated: NotifyDataUpdated => Future[Unit]

  private var _data: ops.DInner = initialData.asInstanceOf[ops.DInner]
  def data: ops.DInner = _data

  def relatedDataUpdated(relatedId: String, related: Data): Unit = ()

  def combine(otherData: ops.DInner): Future[Unit] = {
    _data = ops.combine(_data, otherData)
    notifyDataUpdated(NotifyDataUpdated(id, data))
  }
}

trait EntityFacade extends Entity {
  /**
    * Request explicit data difference from entity
    * @param dataClock
    * @return
    */
  def getUpdatesFrom(dataClock: String)(implicit timeout: Timeout): Future[Data]

  /**
    * Receives updates of related external data
    * @param relatedId
    * @param relatedData
    * @return
    */
  def onUpdate(relatedId: String, relatedData: Data)(implicit timeout: Timeout): Future[Unit]
}

abstract class ActorFacade extends EntityFacade {
  /**
    * Actor which manage an entity's data
    */
  protected val holder: ActorRef

  def getUpdatesFrom(dataClock: String)(implicit timeout: Timeout): Future[Data] =
    holder.ask(GetDifferenceFrom(id, dataClock)).mapTo[Data]

  def onUpdate(relatedId: String, relatedData: Data)(implicit timeout: Timeout): Future[Unit] =
    holder.ask(RelatedDataUpdated(id, relatedId, relatedData)).mapTo[Unit]
}

trait ActorDataMethods { this: Actor =>
  def handleDataMessage(entity: AbstractEntity): Receive = {
    case GetDifferenceFrom(id, olderClock) if id == entity.id ⇒
      sender() ! entity.ops.diffFromClock(entity.data, olderClock)

    case RelatedDataUpdated(id, relatedId, relatedUpdate) if id == entity.id ⇒
      entity.relatedDataUpdated(relatedId, relatedUpdate)
      sender() ! ()
  }
}