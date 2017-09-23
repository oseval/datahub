package ru.oseval.datahub

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import ru.oseval.datahub.data.Data
import ru.oseval.datahub.data.Data.{GetDifferenceFrom, RelatedDataUpdated}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

case class ActorFacade(entity: Entity, holder: ActorRef) extends EntityFacade {
  override def getUpdatesFrom(dataClock: entity.ops.D#C)(implicit timeout: FiniteDuration): Future[entity.ops.D] =
    holder.ask(GetDifferenceFrom(entity.id, dataClock))(timeout).asInstanceOf[Future[entity.ops.D]]

  override def onUpdate(relatedId: String, relatedData: Data)(implicit timeout: FiniteDuration): Future[Unit] =
    holder.ask(RelatedDataUpdated(entity.id, relatedId, relatedData))(timeout).mapTo[Unit]
}

trait ActorDataMethods { this: Actor =>
  protected val storage: LocalDataStorage

  def handleDataMessage(entity: Entity): Receive = {
    case GetDifferenceFrom(id, olderClock) if id == entity.id =>
      sender() ! storage.diffFromClock(entity, olderClock)

    case RelatedDataUpdated(id, relatedId, relatedUpdate) if id == entity.id =>
      storage.combine(relatedId, relatedUpdate)
      sender() ! ()
  }
}
