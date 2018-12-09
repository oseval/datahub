package ru.oseval.datahub.domain

import ru.oseval.datahub.Entity
import ru.oseval.datahub.data._

case class GroupMessage(text: String)

case class Group(groupId: String,
                 title: Option[String],
                 members: SetData[Long, Long],
                 messages: SetData[GroupMessage, Long],
                 clock: Long = 0L) extends Data {
  override type C = Long
}

object GroupOpsSimple extends DataOps {
  override type D = Group
  override val ordering: Ordering[Long] = Ordering.Long
  override val zero: Group = Group("", None, SetDataOps.zero, SetDataOps.zero)

  override def nextClock(current: Long): Long = System.currentTimeMillis max (current + 1L)

  override def getRelations(data: Group): (Set[Entity], Set[Entity]) =
    data.members.elements.toSet[Long].map(UserEntity(_): Entity) -> Set.empty[Entity]
}

object GroupOps extends ALODataOps[GroupOpsSimple.type] {
  override protected val ops = GroupOpsSimple
}

case class GroupEntity(groupId: String) extends Entity {
  lazy val id: String = "group_" + groupId
  override val ops = GroupOps
}