package com.github.oseval.datahub.cluster

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Timers}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import com.github.oseval.datahub.cluster.ActorDatasource._
import com.github.oseval.datahub.data.{Data, DataOps}
import com.github.oseval.datahub.{Entity, RemoteEntityFacade, Subscriber}

import scala.collection.mutable

object ActorDatasource {
  case class SyncRemoteData(entityId: String, dataClock: Any)

  case class OnSubscribe(entityId: String, subscriber: Subscriber)
  case class OnUnsubscribe(entityId: String, subscriber: Subscriber)

  sealed trait ActorDataSourceCommand {
    val entityId: String
  }
  case class OnUpdate(entityId: String, update: Data) extends ActorDataSourceCommand
  case class SubscribeDataSource(entityId: String) extends ActorDataSourceCommand
  case class UnsubscribeDataSource(entityId: String) extends ActorDataSourceCommand
  case class Subscribed(entityId: String) extends ActorDataSourceCommand

  case object DataSourceSubscriptionsShardTerminated

  trait ShardedEntity { this: Actor =>
    protected val entity: Entity
    protected def curData: entity.ops.D
    private lazy val subscriptionsShard: ActorRef = DataSourceSubscriptionsShard.start(entity.ops.kind + "_datasource", context.system)

    protected def handleDataCommand: Receive = {
      case SyncRemoteData(entityId, clock) if entity.id == entityId =>
        entity.ops.matchClock(clock).foreach { clk =>
          subscriptionsShard ! OnUpdate(entity.id, entity.ops.diffFromClock(curData, clk))
        }
    }

    protected def dataUpdated(): Unit =
      subscriptionsShard ! OnUpdate(entity.id, curData)
  }
}

private object DataSourceSubscriptionsShard {
  private def extractEntityId: ShardRegion.ExtractEntityId = {
    case cmd: ActorDataSourceCommand => (cmd.entityId, cmd)
  }

  private def extractShardId: ShardRegion.ExtractShardId = {
    case cmd: ActorDataSourceCommand => (cmd.entityId.hashCode % 100).toString
  }

  def start(kind: String, system: ActorSystem): ActorRef =
    ClusterSharding(system).start(
      typeName = kind + "_subscriptions",
      entityProps = Props(classOf[DataSourceSubscriptionsActor]),
      settings = ClusterShardingSettings(system),
      extractEntityId = DataSourceSubscriptionsShard.extractEntityId,
      extractShardId = DataSourceSubscriptionsShard.extractShardId
    )
}

private class DataSourceSubscriptionsActor extends Actor {
  val subscriptions = mutable.Map.empty[String, mutable.Set[ActorRef]]

  def receive: Receive = {
    case u: OnUpdate =>
      subscriptions.get(u.entityId).foreach(_.foreach(_ ! u))

    case SubscribeDataSource(entityId) =>
      val subs = subscriptions.getOrElseUpdate(entityId, mutable.Set.empty)
      subs += sender()

      sender() ! Subscribed(entityId)

    case UnsubscribeDataSource(entityId) =>
      subscriptions.get(entityId).foreach { subs =>
        subs -= sender()
        if (subs.isEmpty) subscriptions -= entityId
      }
  }
}

/**
  * Concrete implementation should be registered in datahub outside
  *
  * @param shardedDataRegion
  * @param onDataUpdate
  * @param system
  */
abstract class ShardedDataSource(shardedDataRegion: ActorRef, val ops: DataOps, onDataUpdate: (String, Data) => Unit)
                                (implicit system: ActorSystem)
  extends RemoteEntityFacade {

  private val impl: ActorRef =
    system.actorOf(Props(classOf[ShardedDataSourceActor], onDataUpdate), ops.kind + "_datasource")

  override def syncData(entityId: String, dataClock: ops.D#C): Unit =
    shardedDataRegion ! SyncRemoteData(entityId, dataClock)

  override def onSubscribe(entity: Entity, subscriber: Subscriber, lastKnownDataClock: Any): Unit =
    if (entity.ops.kind == ops.kind) impl ! OnSubscribe(entity.id, subscriber)

  override def onUnsubscribe(entity: Entity, subscriber: Subscriber): Unit =
    if (entity.ops.kind == ops.kind) impl ! OnUnsubscribe(entity.id, subscriber)
}

private class ShardedDataSourceActor(onDataUpdate: (String, Data) => Unit) extends Actor with Timers {
  import scala.concurrent.duration._

  val subscriptionsShard = DataSourceSubscriptionsShard.start(self.path.name, context.system)
  val subscriptions = mutable.Map.empty[String, mutable.Set[Subscriber]]
  val pendingSubscriptions = mutable.Set.empty[String]

  case object Tick
  timers.startPeriodicTimer("ticker", Tick, 1.second)

  def receive: Receive = {
    case Tick =>
      pendingSubscriptions.foreach(entityId => subscriptionsShard ! SubscribeDataSource(entityId))

    case OnSubscribe(entityId, subscriber) =>
      val subs = subscriptions.getOrElseUpdate(entityId, mutable.Set.empty[Subscriber])
      subs += subscriber

      pendingSubscriptions += entityId

      subscriptionsShard ! SubscribeDataSource(entityId)

    case OnUnsubscribe(entityId, subscriber) =>
      subscriptions.get(entityId).foreach { subs =>
        subs -= subscriber
        if (subs.isEmpty) subscriptions -= entityId
      }
      subscriptionsShard ! UnsubscribeDataSource(entityId)

    case Subscribed(entityId) =>
      pendingSubscriptions -= entityId
      context.watchWith(sender(), DataSourceSubscriptionsShardTerminated)

    case OnUpdate(entityId, data) =>
      onDataUpdate(entityId, data)

    case DataSourceSubscriptionsShardTerminated =>
      subscriptions.foreach(entityId => subscriptionsShard ! SubscribeDataSource(entityId._1))
  }
}
