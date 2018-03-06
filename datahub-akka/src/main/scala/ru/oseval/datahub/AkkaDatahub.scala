package ru.oseval.datahub

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import ru.oseval.datahub.AsyncDatahub.Storage
import ru.oseval.datahub.data.Data

import scala.concurrent.{ExecutionContext, Future}

object AkkaDatahub {
  private case class RelationAndData(entity: Entity)(val data: entity.ops.D)
  private sealed trait DatahubCommand {
    val entityId: String
  }
  private case class Register(facade: EntityFacade) extends DatahubCommand {
    val entityId = facade.entity.id
  }
  private case class Subscribe(entityId: String,
                               subscriberId: String,
                               subscriberKind: String,
                               entityClockOpt: Option[Any]) extends DatahubCommand
  private case class Unsubscribe(entityId: String, subscriberId: String) extends DatahubCommand
  private case class SendChangeToOne(entityId: String,
                                     entityKind: String,
                                     relationAndData: RelationAndData) extends DatahubCommand
  private case class DataUpdated(entityId: String, data: Data) extends DatahubCommand
  private case class SyncRelation()

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

  private class LocalDatahub(storage: Storage)
                            (implicit system: ActorSystem, ec: ExecutionContext)
    extends AsyncDatahub(storage)(ec) {

    private lazy val region = ClusterSharding(system).shardRegion(shardingName)

    override protected def sendChangeToOne(entity: Entity, subscriberId: String)
                                          (entityData: entity.ops.D): Option[Future[Unit]] =
      super.sendChangeToOne(entity, subscriberId)(entityData).orElse {
        None
      }
  }

  private def props(storage: Storage) = Props(classOf[AkkaDatahubActor], storage)

  private class AkkaDatahubActor(storage: Storage) extends Actor with ActorLogging {
    private val localDatahub = new LocalDatahub(storage)(context.system, context.dispatcher)
    override def receive: Receive = {
      case Register(facade) =>
        // TODO: just for adding facade to inner storage (take it explicitly?)
        localDatahub.register(facade)(facade.entity.ops.zero.clock, Map.empty, Set.empty)
      case Subscribe(entityId, subscriberId, subscriberKind, lastKnownDataClockOpt) =>
        localDatahub.subscribe(entityId, subscriberId, subscriberKind, lastKnownDataClockOpt)
      case Unsubscribe(entityId, subscriberId) =>
        localDatahub.unsubscribe(entityId, subscriberId)
      case DataUpdated(entityId, data) =>
        // subscribers is present now
        localDatahub.notifySubscribers(facade.entity, Set.empty)(data)
      case SendChangeToOne(entityId, entityKind, relationAndData) =>
        // it is possible that entity is not registered yet
        localDatahub.sendChangeToOne(relationAndData.entity, entityId, entityKind)(relationAndData.data)
    }
  }
}

case class AkkaDatahub(storage: Storage)
                      (implicit system: ActorSystem, ec: ExecutionContext)
  extends AsyncDatahub(storage)(ec) {

  import AkkaDatahub._

  private lazy val region: ActorRef = ClusterSharding(system).start(
    typeName = AkkaDatahub.shardingName,
    entityProps = props(storage),
    settings = ClusterShardingSettings(system),
    extractEntityId = extractEntityId,
    extractShardId = extractShardId)

  override def register(facade: EntityFacade)
                       (lastClock: facade.entity.ops.D#C,
                        relationClocks: Map[Entity, Any],
                        forcedSubscribers: Set[Entity]): Future[Unit] = {
    region ! Register(facade)
    super.register(facade)(lastClock, relationClocks, forcedSubscribers)
  }

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
    // TODO: force subscribers could be not registered yet - need store them for future purposes (add to inner storage)
    // TODO: !!!but it must be done on the shard entity side!!!
    forcedSubscribers.foreach(sendChangeToOne(entity, _)(data))

    region ! DataUpdated(entity.id, data)
  }

  override protected def sendChangeToOne(entity: Entity, subscriberId: String, subscriberKind: String)
                                        (entityData: entity.ops.D): Option[Future[Unit]] =
    super.sendChangeToOne(entity, subscriberId, subscriberKind)(entityData).orElse {
      region ! SendChangeToOne(subscriberId, subscriberKind, RelationAndData(entity)(entityData))

      None
    }

  override def syncRelationClocks(entityId: String, relationClocks: Map[String, Any]): Future[Unit] = {
    relationClocks.foreach { case (rid, clock) => region ! SyncRelation(entityId, rid, clock) }
  }
}
