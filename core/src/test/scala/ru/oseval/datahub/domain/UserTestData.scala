package ru.oseval.datahub.domain

import ru.oseval.datahub.Entity
import ru.oseval.datahub.data.AIDataOps

case class User(id: Int, name: String)

object UserOps extends AIDataOps[User]()

case class UserEntity(userId: Long) extends Entity {
  lazy val id: String = "user_" + userId
  override val ops = UserOps
}