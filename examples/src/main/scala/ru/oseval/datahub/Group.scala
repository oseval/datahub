package ru.oseval.datahub

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import org.slf4j.LoggerFactory
import ru.oseval.datahub.User.{UserEntity, UserOps}
import ru.oseval.datahub.data._

import scala.concurrent.Future
import scala.concurrent.duration._

case class Group(id: String, title: String, members: Set[Long])

object Group {
  def props(id: String, title: String, notifier: ActorRef): Props =
    Props(classOf[GroupActor], id, title, notifier)

  case class AddMember(userId: Long)
  case object GetMembers

  // CompoundData as wrapper need so much time - todo it

  // it must be at-least-once due to SetData inside
  case class GroupData(title: String,
                       memberSet: SetData[Long, Long],
                       clock: Long
                      ) extends AtLeastOnceData {
    // due to memberSet is a base for this data, then his clock should be increased on each data update
    // but SetData has no such ability and therefore GroupData should have its own clock
    val previousClock: Long = memberSet.previousClock
    val isSolid: Boolean = memberSet.isSolid
    override type C = Long
    lazy val members = memberSet.elements.toSet
  }

  object GroupOps extends DataOps {
    override type D = GroupData
    override val ordering: Ordering[Long] = Ordering.Long
    override val zero: GroupData = GroupData("", SetDataOps.zero(ClockInt(0L, 0L), ordering))

    override def combine(a: GroupData, b: GroupData): GroupData = {
      val second = if (ordering.gt(a.clock, b.clock)) a else b
      GroupData(
        title = second.title,
        memberSet = SetDataOps.combine(a.memberSet, b.memberSet),
        clock =
      )
    }

    override def nextClock(current: Long): Long =
      System.currentTimeMillis max (current + 1L)

    override def diffFromClock(a: GroupData, from: Long): GroupData =
      a.copy(memberSet = SetDataOps.diffFromClock(a.memberSet, from))(a.clock)
    override def getRelations(data: GroupData): Set[String] = data.members.map(UserEntity(_).id)
  }

  case class GroupEntity(groupId: String) extends Entity {
    lazy val id: String = "group_" + groupId
    override val ops = GroupOps
  }
}

private class GroupActor(id: String, title: String, notifier: ActorRef)
  extends Actor with ActorDataMethods[Future] {
  import Group._
  import context.dispatcher
  private val log = LoggerFactory.getLogger(getClass)

  private implicit val timeout: Timeout = 3.seconds
  private val group = GroupEntity(id)
  protected val storage = new LocalDataStorage(log, ActorFacade(_, self), AsyncDatahub)

  {
    implicit val cint: ClockInt[Long] = ClockInt(0L, System.currentTimeMillis)
    storage.addEntity(group)(GroupData(title, SetDataOps.zero[Long, Long]))
  }

  // TODO: force register user facades after start - to subscribe users on this group
  // TODO: UserOps.makeActorFacade(userId) == ActorFacade(UserEntity(..), userActorRef)

  override def receive: Receive = handleDataMessage(group) orElse {
    case GetMembers => sender() ! storage.get(group).map(_.members.toSet).getOrElse(Set.empty)
    case AddMember(userId) =>
      storage.addRelation(UserEntity(userId))

      storage.updateEntity(group) { implicit clockInt => g =>
        g.copy(memberSet = g.memberSet.add(userId))
      } pipeTo sender()
  }
}