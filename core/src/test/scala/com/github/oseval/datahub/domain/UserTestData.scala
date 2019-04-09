package com.github.oseval.datahub.domain

import com.github.oseval.datahub.{Entity, LocalEntityFacade}
import com.github.oseval.datahub.data.{AIDataOps, SetData}

case class User(id: Int, name: String, groupId: String, messages: SetData[GroupMessage, Long])

object UserOps extends AIDataOps[User]

case class UserEntity(userId: Long) extends Entity {
  lazy val id: String = "user_" + userId
  override val ops = UserOps
}

case class UserEntityFacade(entity: Entity) extends LocalEntityFacade {
  /**
    * Request explicit data difference from entity
    *
    * @param dataClock
    * @return
    */
  override def syncData(dataClock: entity.ops.D#C): Unit = ???
}
