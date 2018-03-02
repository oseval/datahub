package ru.oseval.datahub

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import ru.oseval.datahub.AsyncDatahub.{MemoryInnerStorage, Storage}
import ru.oseval.datahub.data.Data

import scala.concurrent.{ExecutionContext, Future}

object AkkaDatahub {
  private sealed trait DatahubCommand {
    val entityId: String
  }
  private case class Subscribe(entityId: String,
                               subscriberId: String,
                               subscriberKind: String,
                               entityClockOpt: Option[Any]) extends DatahubCommand
  private case class Unsubscribe(entityId: String, subscriberId: String) extends DatahubCommand
  private case class SendChangeToOne(entityId: String,
                                     relationId: String,
                                     relationData: Data) extends DatahubCommand
  private case class DataUpdated(entityId: String, data: Data) extends DatahubCommand

  // sharding
  val shardingName = "Entities"

  private val extractEntityId: ShardRegion.ExtractEntityId = {
    case c: DatahubCommand => (c.entityId, c)
  }

  private val numberOfShards = 100

  private val extractShardId: ShardRegion.ExtractShardId = {
    case c: DatahubCommand ⇒ (c.entityId.## % numberOfShards).toString
    case ShardRegion.StartEntity(id) => // TODO: ???
      // StartEntity is used by remembering entities feature
      (id.toLong % numberOfShards).toString
  }

  private class ShardEntityDatahub(storage: Storage)
                                  (implicit system: ActorSystem, ec: ExecutionContext)
    extends AsyncDatahub(storage)(ec) {

    private lazy val region = ClusterSharding(system).shardRegion(shardingName)

    override protected def sendChangeToOne(entity: Entity, subscriberId: String)
                                          (entityData: entity.ops.D): Future[Unit] =
      innerStorage.facade(subscriberId).map(_.onUpdate(entity.id, entityData)) getOrElse {
        region ! SendChangeToOne(subscriberId, entity.id, entityData)
      }
  }

  private def props(storage: Storage) = Props(classOf[AkkaDatahubActor], storage)

  private class AkkaDatahubActor(storage: Storage) extends Actor with ActorLogging {
    private val localDatahub = new ShardEntityDatahub(context.system)(context.dispatcher)
    override def receive: Receive = {
      case Subscribe(entityId, subscriberId, subscriberKind, lastKnownDataClockOpt) =>
        localDatahub.subscribe(entityId, subscriberId, subscriberKind, lastKnownDataClockOpt)
      case Unsubscribe(entityId, subscriberId) =>
        localDatahub.unsubscribe(entityId, subscriberId)
      case DataUpdated(entityId, data) =>
        localDatahub.dataUpdated(entityId, data, Set.empty, Set.empty, Set.empty)
    }
  }
}

case class AkkaDatahub(storage: Storage)
                      (implicit system: ActorSystem, ec: ExecutionContext)
  extends AsyncDatahub(storage)(ec) {

  import AkkaDatahub._

  private val actorDatahubImpl = new ShardEntityDatahub(storage: Storage)

  private lazy val region: ActorRef = ClusterSharding(system).start(
    typeName = AkkaDatahub.shardingName,
    entityProps = props(storage),
    settings = ClusterShardingSettings(system),
    extractEntityId = extractEntityId,
    extractShardId = extractShardId)

  override protected def subscribe(entityId: String,
                                   subscriberId: String,
                                   subscriberKind: String,
                                   lastKnownDataClockOpt: Option[Any]): Unit = {
    log.debug("subscribe {}, {}, {}", subscriberId, entityId, innerStorage.facade(entityId))

    region ! Subscribe(entityId, subscriberId, subscriberKind, lastKnownDataClockOpt)
  }

  override protected def unsubscribe(entityId: String, subscriberId: String): Unit =
    region ! Unsubscribe(entityId, subscriberId)

  protected def notifySubscribers(entity: Entity, forcedSubscribers: Set[String])(data: entity.ops.D): Unit = {
    forcedSubscribers.foreach(subscriberId =>
      innerStorage.facade(subscriberId).map(_.onUpdate(entity.id, data))
    )
    region ! DataUpdated(entity.id, data)
  }

  // TODO: to shard
  override protected def sendChangeToOne(toId: String, relation: Entity)(relationData: relation.ops.D): Future[Unit] =
    innerStorage.facade(toId).map(_.onUpdate(relation.id, relationData)) getOrElse {
      region ! SendChangeToOne(toId, relation.id, relationData)
    }

  override def syncRelationClocks(entityId: String, relationClocks: Map[String, Any]): Future[Unit] = {
    relationClocks.foreach { case (rid, clock) => region ! SyncRelation(entityId, rid, clock) }
  }
}
