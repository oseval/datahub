package ru.oseval.datahub.domain

import ru.oseval.datahub.Entity
import ru.oseval.datahub.data.{ALODataOps, SetData}

case class GroupMessage(text: String)

case class Group(groupId: String,
                 title: Option[String],
                 members: SetData[Long, Long],
                 messages: SetData[GroupMessage, Long])

object GroupOps extends ALODataOps[Group]

case class GroupEntity(groupId: String) extends Entity {
  lazy val id: String = "group_" + groupId
  override val ops = GroupOps
}