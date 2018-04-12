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
  private[datahub] case class RequestForApprove(entityId: String, relationId: String) extends FacadeMessage
  private[datahub] case class OnSubscribe(relationId: String) extends FacadeMessage
}
import ActorFacadeMessages._

case class ActorFacade(entity: Entity,
                       holder: ActorRef,
                       untrustedKinds: Set[String] = Set.empty[String]
                      )(implicit timeout: FiniteDuration) extends EntityFacade {
  override def getUpdatesFrom(dataClock: entity.ops.D#C): Future[entity.ops.D] =
    holder.ask(GetDifferenceFrom(entity.id, dataClock)).asInstanceOf[Future[entity.ops.D]]

  override def onUpdate(relatedId: String, relatedData: Data): Future[Unit] =
    holder.ask(RelatedDataUpdated(entity.id, relatedId, relatedData)).mapTo[Unit]

  override def requestForApprove(subscriber: Entity): Future[Boolean] = {
    println(("ZZZZZ", subscriber, entity))
    if (entity.untrustedKinds contains subscriber.ops.kind) {
      println(("ASK HOLDER", holder, entity))
      holder.ask(RequestForApprove(entity.id, subscriber.id)).mapTo[Boolean]
    } else Future.successful(true)
  }

  override def onSubscribe(relation: Entity): Future[Unit] =
    holder.ask(OnSubscribe(relation.id)).mapTo[Unit]
}

trait ActorDataMethods[M[_]] { this: Actor =>
  protected val storage: LocalDataStorage[M]

  def handleDataMessage(entity: Entity): Receive = {
    case GetDifferenceFrom(id, olderClock) if id == entity.id =>
      sender() ! storage.diffFromUnknownClock(entity, olderClock)

    case RelatedDataUpdated(id, relatedId, relatedUpdate) if id == entity.id =>
      storage.combineRelation(relatedId, relatedUpdate)
      sender() ! ()

    case RequestForApprove(id, relationId) if id == entity.id =>
      sender() ! storage.approveRelation(entity, relationId)

    case OnSubscribe(relationId) =>
      sender() ! storage.
  }
}
