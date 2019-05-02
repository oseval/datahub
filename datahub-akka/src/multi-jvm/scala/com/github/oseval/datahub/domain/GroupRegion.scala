package com.github.oseval.datahub.domain

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import UserRegion._
import com.github.oseval.datahub.data.AIData
import com.github.oseval.datahub.domain.GroupRegion.{SetProbe, Updated}
import com.github.oseval.datahub.{Datahub, RelationsManager}
import org.slf4j.LoggerFactory

import scala.ref.WeakReference

object GroupRegion {
  private def extractEntityId: ShardRegion.ExtractEntityId = {
    case (groupId: String, e) => (groupId, e)
    case set @ SetProbe(groupId, _) => (groupId, set)
  }

  private def extractShardId: ShardRegion.ExtractShardId = {
    case (groupId: String, e) ⇒ (groupId.hashCode % 100).toString
    case SetProbe(groupId, _) => (groupId.hashCode % 100).toString
  }

  private val typeName = "Group"

  def start(system: ActorSystem, dh: Datahub) =
    ClusterSharding(system).start(
      typeName = typeName,
      entityProps = Props(classOf[GroupActor], dh),
      settings = ClusterShardingSettings(system),
      extractEntityId = extractEntityId,
      extractShardId = extractShardId
    )

  case class SetProbe(groupId: String, probe: ActorRef)
  case class Updated(entityId: String, dt: AIData[User])
}

private[datahub] class GroupActor(dh: Datahub) extends Actor with ActorLogging {
  val userIds = Set(1, 2, 3)
  var probe: ActorRef = _
  val manager =
    new RelationsManager(
      WeakReference(dh),
      LoggerFactory.getLogger(getClass),
      (rel, dt) => {
        dt match {
          case d: AIData[User] => probe ! Updated(rel.id, d)
          case _ =>
        }
      }
    )

  override def receive: Receive = {
    case SetProbe(_, ref) =>

      if (probe == null) userIds.foreach(userId => manager.subscribeOnRelation(UserEntity(userId)))

      probe = ref
      ref ! ()
  }

  override def postStop(): Unit = {
    userIds.foreach(userId => manager.removeRelation(UserEntity(userId).id))
  }
}