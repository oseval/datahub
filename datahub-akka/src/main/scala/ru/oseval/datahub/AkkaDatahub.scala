package ru.oseval.datahub

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import akka.pattern.{ask, pipe}
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
                               subscriber: Entity,
                               entityClockOpt: Option[Any]) extends DatahubCommand {
    val entityId: String = entity.id
  }
  private case class Unsubscribe(entity: Entity, subscriber: Entity) extends DatahubCommand {
    val entityId: String = entity.id
  }
  private case class RelationDataUpdated(entity: Entity,
                                         relationAndData: RelationAndData) extends DatahubCommand {
    val entityId: String = entity.id
  }
  case class DataUpdated(entity: Entity, data: Data) extends DatahubCommand {
    val entityId: String = entity.id
  }
  private case class SyncRelation(entity: Entity, subscriber: Entity, clock: Any) {
    val entityId: String = entity.id
  }

  // sharding
  val shardingName = "Entitiesooo"

  private val extractEntityId: ShardRegion.ExtractEntityId = {
    case c: DatahubCommand =>
      println(("EEEEEE", c))
      (c.entityId, c)
  }

  private val numberOfShards = 100

  private val extractShardId: ShardRegion.ExtractShardId = {
    case c: DatahubCommand ⇒
      println(("CCCC", c))
      (c.entityId.## % numberOfShards).toString
    case ShardRegion.StartEntity(id) => // TODO: ???

      println(("RRRRR", id))
      // StartEntity is used by remembering entities feature
      (id.toLong % numberOfShards).toString
  }

  private class LocalDatahub(storage: Storage)
                            (implicit system: ActorSystem, ec: ExecutionContext)
    extends AsyncDatahub(storage)(ec) {

    val region = ClusterSharding(system).shardRegion(shardingName)

    override def sendChangeToOne(entity: Entity, subscriber: Entity)
                                (entityData: entity.ops.D): Option[Future[Unit]] =
      super.sendChangeToOne(entity, subscriber)(entityData).orElse {
        region ! RelationDataUpdated(subscriber, RelationAndData(entity)(entityData))
        None
      }
  }

  private[datahub] def props(storage: Storage) = Props(classOf[AkkaDatahubActor], storage)

  private class AkkaDatahubActor(storage: Storage) extends Actor with ActorLogging {
    private val localDatahub = new LocalDatahub(storage)(context.system, context.dispatcher)
    override def receive: Receive = {
      case Register(facade) =>
        println(("REGREGF", facade.entity))
        // TODO: just for adding facade to inner storage (take it explicitly?)
        localDatahub.register(facade)(facade.entity.ops.zero.clock, Map.empty, Set.empty)
      case Subscribe(entity, subscriber, lastKnownDataClockOpt) =>
        println(("SUBSUB", entity, subscriber))
        localDatahub.subscribe(entity, subscriber, lastKnownDataClockOpt) pipeTo sender()
      case Unsubscribe(entity, subscriber) =>
        localDatahub.unsubscribe(entity, subscriber)
      case DataUpdated(entity, data) =>
        // subscribers is present now
        // TODO: drop asInstanceOf
        localDatahub.dataUpdated(entity, Set.empty)(data.asInstanceOf[entity.ops.D])
      case RelationDataUpdated(entity, relationAndData) =>
        // it is possible that entity is not registered yet
        // TODO:
        localDatahub.sendChangeToOne(relationAndData.entity, entity)(relationAndData.data)
      case SyncRelation(entity, subscriber, clock) =>
        localDatahub.syncRelationClocks(subscriber, Map(entity -> clock))
    }
  }
}

case class AkkaDatahub(storage: Storage)
                      (implicit system: ActorSystem, ec: ExecutionContext)
  extends AsyncDatahub(storage)(ec) {

  import AkkaDatahub._

  private val region: ActorRef = ClusterSharding(system).start(
    typeName = AkkaDatahub.shardingName,
    entityProps = props(storage),
    settings = ClusterShardingSettings(system),
    extractEntityId = extractEntityId,
    extractShardId = extractShardId)

  override def register(facade: EntityFacade)
                       (lastClock: facade.entity.ops.D#C,
                        relationClocks: Map[Entity, Any],
                        forcedSubscribers: Set[EntityFacade]): Future[Unit] = {
    println(("NNNNN", facade.entity))
    region ! Register(facade)
    super.register(facade)(lastClock, relationClocks, forcedSubscribers)
  }

  // TODO: pending subscriptions
  override def subscribe(entity: Entity,
                         subscriber: Entity,
                         lastKnownDataClockOpt: Option[Any]): Future[Unit] = {
    log.debug("subscribe {}, {}, {}", subscriber.id, entity.id, entity)

    (region ? Subscribe(entity, subscriber, lastKnownDataClockOpt)).mapTo[Unit]
  }

  override def unsubscribe(entity: Entity, subscriber: Entity): Future[Unit] = {
    region ! Unsubscribe(entity, subscriber)
    Future.unit
  }

  override def notifySubscribers(entity: Entity, forcedSubscribers: Set[EntityFacade])(data: entity.ops.D): Unit = {
    forcedSubscribers.foreach(_.onUpdate(entity.id, data))

    region ! DataUpdated(entity, data)
  }

  override def sendChangeToOne(entity: Entity, subscriber: Entity)
                              (entityData: entity.ops.D): Option[Future[Unit]] =
    super.sendChangeToOne(entity, subscriber)(entityData).orElse {
      region ! RelationDataUpdated(subscriber, RelationAndData(entity)(entityData))

      None
    }

  override def syncRelationClocks(entity: Entity, relationClocks: Map[Entity, Any]): Unit =
    relationClocks.foreach { case (relation, clock) => region ! SyncRelation(relation, entity, clock) }
}
