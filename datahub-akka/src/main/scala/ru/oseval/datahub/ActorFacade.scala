package ru.oseval.datahub

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import ru.oseval.datahub.data.Data

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object ActorFacadeMessages {
  private[datahub] sealed trait FacadeMessage
  private[datahub] case class GetDifferenceFrom(entity: String, dataClock: Any) extends FacadeMessage
  private[datahub] case class RelatedDataUpdated(toEntityId: String, relatedId: String, data: Data) extends FacadeMessage
  private[datahub] case class RequestForApprove(entityId: String, relation: Entity) extends FacadeMessage
}
import ActorFacadeMessages._

case class ActorFacade(entity: Entity,
                       holder: ActorRef,
                       untrustedKinds: Set[String] = Set.empty[String]
                      ) extends EntityFacade {
  override def getUpdatesFrom(dataClock: entity.ops.D#C)(implicit timeout: FiniteDuration): Future[entity.ops.D] =
    holder.ask(GetDifferenceFrom(entity.id, dataClock))(timeout).asInstanceOf[Future[entity.ops.D]]

  override def onUpdate(relatedId: String, relatedData: Data)(implicit timeout: FiniteDuration): Future[Unit] =
    holder.ask(RelatedDataUpdated(entity.id, relatedId, relatedData))(timeout).mapTo[Unit]

  override def requestForApprove(relation: Entity)(implicit timeout: FiniteDuration): Future[Boolean] =
    if (entity.untrustedKinds contains relation.kind)
      holder.ask(RequestForApprove(entity.id, relation))(timeout).mapTo[Boolean]
    else Future.successful(true)
}

trait ActorDataMethods { this: Actor =>
  protected val storage: LocalDataStorage

  def handleDataMessage(entity: Entity): Receive = {
    case GetDifferenceFrom(id, olderClock) if id == entity.id =>
      sender() ! storage.diffFromUnknownClock(entity, olderClock)

    case RelatedDataUpdated(id, relatedId, relatedUpdate) if id == entity.id =>
      storage.combineRelation(relatedId, relatedUpdate)
      sender() ! ()

    case RequestForApprove(id, relation) if id == entity.id =>
      sender() ! storage.approveRelation(entity, relation)
  }
}
