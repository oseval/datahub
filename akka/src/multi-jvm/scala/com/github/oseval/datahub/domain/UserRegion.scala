package com.github.oseval.datahub.domain

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import com.github.oseval.datahub.Entity
import com.github.oseval.datahub.cluster.ActorDatasource
import com.github.oseval.datahub.cluster.ActorDatasource.ShardedEntity
import com.github.oseval.datahub.data.{AIData, AIDataOps}

object UserRegion {
  private def extractEntityId: ShardRegion.ExtractEntityId = {
    case (userId: String, e) => (userId, e)
    case u @ UpdateName(entityId, _) => (entityId.stripPrefix("user_"), u)
    case sync: ActorDatasource.SyncRemoteData => sync.entityId.stripPrefix("user_") -> sync
  }

  private def extractShardId: ShardRegion.ExtractShardId = {
    case (userId: String, e) ⇒ (userId.hashCode % 100).toString
    case UpdateName(entityId, _) => (entityId.hashCode % 100).toString
    case sync: ActorDatasource.SyncRemoteData => (sync.entityId.hashCode % 100).toString
  }

  private val typeName = "User"

  def start(system: ActorSystem): ActorRef =
    ClusterSharding(system).start(
      typeName = typeName,
      entityProps = Props(classOf[UserActor]),
      settings = ClusterShardingSettings(system),
      extractEntityId = extractEntityId,
      extractShardId = extractShardId
    )

  case class User(name: String)
  object UserDataOps extends AIDataOps[User]() {
    override val kind: String = "user"
  }
  case class UserEntity(userId: Int) extends Entity {
    override val ops: UserDataOps.type = UserDataOps
    override val id: String = ops.kind + "_" + userId.toString
  }

  case class UpdateName(entityId: String, newName: String)
}
import com.github.oseval.datahub.domain.UserRegion._

private class UserActor extends Actor with ActorLogging with ShardedEntity {
  protected val entity = UserEntity(self.path.name.stripPrefix("user_").toInt)
  protected var curData: AIData[User] = AIData(User("Name_" + self.path.name), 100)

  override def receive: Receive = handleDataCommand orElse {
    case UpdateName(_, newName) =>
      curData = AIData(User(newName), System.currentTimeMillis)
      dataUpdated()
  }
}

//"""
// + 1) group send subscribe to localDatahub1
// + 2) group send unsubscribe on stop
//
// + 3) localDatahub1 send onSubscribe to userDatasource1
// + 4) userDatasource1 remember subscription userId -> [subscriber]
//
// + 5) userDatasource1 send onSubscribe to userSubscribersShard(per userId) (store subscribed datasources)
// + 6) userDatasource1 watch userSubscriberShard and resubscribe on termination
// + 7) userSubscriberShard watch userDataSource1 and unsubscribe it on termination ???
//   - userDataSource1 should never terminated, just remove terminated datasourceActors
//
// + 8) localDatahub1 send syncData to userDatasource1
// + 9) userDatasource1 send syncData to userActor
// + 10) userActor send dataUpdated to userSubscribersShard
// + 12) userSubscriberShard send onUpdate to userDatasource1
// + 14) userDatasource1 send dataUpdated to localDatahub1
// + 15) localDatahub1 send onUpdate to group
//
//"""