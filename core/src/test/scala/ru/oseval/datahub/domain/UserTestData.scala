package ru.oseval.datahub.domain

import ru.oseval.datahub.{Entity, EntityFacade}
import ru.oseval.datahub.data.{AIDataOps, Data, SetData}

import scala.concurrent.Future

case class User(id: Int, name: String, groupId: String, messages: SetData[GroupMessage, Long])

object UserOps extends AIDataOps[User](u => Set(GroupEntity(u.groupId).lift) -> Set.empty[Entity])

case class UserEntity(userId: Long) extends Entity {
  lazy val id: String = "user_" + userId
  override val ops = UserOps
}

case class UserEntityFacade(entity: Entity) extends EntityFacade {
  /**
    * Request explicit data difference from entity
    *
    * @param dataClock
    * @return
    */
  override def getUpdatesFrom(dataClock: entity.ops.D#C): Future[entity.ops.D] = ???
}