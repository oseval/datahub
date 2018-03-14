package ru.oseval.datahub

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import ru.oseval.datahub.AsyncDatahub.Storage
import ru.oseval.datahub.data.Data

import scala.concurrent.{ExecutionContext, Future}

private[datahub] object AkkaDatahub {
  private case class RelationAndData(entity: Entity)(_data: Data) {
    val data: entity.ops.D = _data.asInstanceOf[entity.ops.D]
  }
  sealed trait DatahubCommand {
    val entityId: String
  }
  case class Register(facade: EntityFacade) extends DatahubCommand {
    val entityId = facade.entity.id
  }
  private case class Subscribe(entity: Entity,
                               subscriberId: String,
                               subscriberKind: String,
                               entityClockOpt: Option[Any]) extends DatahubCommand {
    val entityId: String = entity.id
  }
  private case class Unsubscribe(entity: Entity, subscriberId: String, subscriberKind: String) extends DatahubCommand {
    val entityId: String = entity.id
  }
  private case class RelationDataUpdated(entityId: String,
                                         entityKind: String,
                                         relationAndData: RelationAndData) extends DatahubCommand
  case class DataUpdated(entity: Entity, data: Data) extends DatahubCommand {
    val entityId: String = entity.id
  }
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

//    private lazy val region = ClusterSharding(system).shardRegion(shardingName)

    override def sendChangeToOne(entity: Entity, subscriberId: String, subscriberKind: String)
                                (entityData: entity.ops.D): Option[Future[Unit]] =
      super.sendChangeToOne(entity, subscriberId, subscriberKind)(entityData).orElse {
        None
      }
  }

  private[datahub] def props(storage: Storage) = Props(classOf[AkkaDatahubActor], storage)

  private class AkkaDatahubActor(storage: Storage) extends Actor with ActorLogging {
    private val localDatahub = new LocalDatahub(storage)(context.system, context.dispatcher)
    override def receive: Receive = {
      case Register(facade) =>
        // TODO: just for adding facade to inner storage (take it explicitly?)
        localDatahub.register(facade)(facade.entity.ops.zero.clock, Map.empty, Set.empty)
      case Subscribe(entity, subscriberId, subscriberKind, lastKnownDataClockOpt) =>
        localDatahub.subscribe(entity, subscriberId, subscriberKind, lastKnownDataClockOpt)
      case Unsubscribe(entity, subscriberId, subscriberKind) =>
        localDatahub.unsubscribe(entity, subscriberId, subscriberKind)
      case DataUpdated(entity, data) =>
        // subscribers is present now
        // TODO: drop asInstanceOf
        localDatahub.notifySubscribers(entity, Set.empty)(data.asInstanceOf[entity.ops.D])
      case RelationDataUpdated(entityId, entityKind, relationAndData) =>
        // it is possible that entity is not registered yet
        // TODO:
        localDatahub.sendChangeToOne(relationAndData.entity, entityId, entityKind)(relationAndData.data)
      case SyncRelations() =>
        localDatahub.syncData(innerStorage.facade(entityId), subscriberId, subscriberKind, lastKnownClocks)
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
                        forcedSubscribers: Set[EntityFacade]): Future[Unit] = {
    region ! Register(facade)
    super.register(facade)(lastClock, relationClocks, forcedSubscribers)
  }

  override def subscribe(entity: Entity,
                         subscriberId: String,
                         subscriberKind: String,
                         lastKnownDataClockOpt: Option[Any]): Unit = {
    log.debug("subscribe {}, {}, {}", subscriberId, entity.id, entity)

    region ! Subscribe(entity, subscriberId, subscriberKind, lastKnownDataClockOpt)
  }

  override protected def unsubscribe(entity: Entity, subscriberId: String, subscriberKind: String): Unit =
    region ! Unsubscribe(entity, subscriberId, subscriberKind)

  protected def notifySubscribers(entity: Entity, forcedSubscribers: Set[Entity])(data: entity.ops.D): Unit = {
    // TODO: force subscribers could be not registered yet - need store them for future purposes (add to inner storage)
    // TODO: !!!but it must be done on the shard entity side!!!
    forcedSubscribers.foreach(e => sendChangeToOne(entity, e.id, e.kind)(data))

    region ! DataUpdated(entity, data)
  }

  override def sendChangeToOne(entity: Entity, subscriberId: String, subscriberKind: String)
                              (entityData: entity.ops.D): Option[Future[Unit]] =
    super.sendChangeToOne(entity, subscriberId, subscriberKind)(entityData).orElse {
      region ! RelationDataUpdated(subscriberId, subscriberKind, RelationAndData(entity)(entityData))

      None
    }

  override def syncRelationClocks(entityId: String, relationClocks: Map[String, Any]): Future[Unit] = {
    relationClocks.foreach { case (rid, clock) => region ! SyncRelation(entityId, rid, clock) }
  }
}
