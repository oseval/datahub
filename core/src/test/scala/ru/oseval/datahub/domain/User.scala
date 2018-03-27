package ru.oseval.datahub.domain

import ru.oseval.datahub.{Entity, EntityFacade}
import ru.oseval.datahub.data.{ACIDataOps, ReplaceableDataOps}

case class User(id: Int, name: String)

object UserOps extends ACIDataOps[User]() with ReplaceableDataOps {
  override def createFacadeFromEntityId(entityId: String): Option[EntityFacade] = None
}

case class UserEntity(userId: Long) extends Entity {
  lazy val id: String = "user_" + userId
  override val ops = UserOps
}
