package ru.oseval.datahub

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import org.slf4j.LoggerFactory
import ru.oseval.datahub.User.UserEntity
import ru.oseval.datahub.data._

import scala.concurrent.duration._

case class Group(id: String, title: String, members: Set[Long])

object Group {
  def props(id: String, title: String, notifier: ActorRef): Props =
    Props(classOf[GroupActor], id, title, notifier)

  case class AddMember(userId: Long)
  case object GetMembers

  // CompoundData as wrapper need so much time - todo it

  // it is must at-least-once due to SetData inside
  case class GroupData(title: String,
                       memberSet: SetData[Long, Long]
                      )(implicit clockInt: ClockInt[Long]) extends CompoundData {
    val clock = clockInt.cur
    val previousClock = clockInt.prev

    override type C = Long
    lazy val members = memberSet.elements.toSet
    protected val children = Set(memberSet)
  }

  object GroupOps extends DataOps {
    override type D = GroupData
    override val ordering: Ordering[Long] = Ordering.Long
    override val zero: GroupData = GroupData("", SetDataOps.zero(ClockInt(0L, 0L), ordering))(ClockInt(0L, 0L))

    override def combine(a: GroupData, b: GroupData): GroupData = {
      val (first, second) = if (ordering.gt(a.clock, b.clock)) (b, a) else (a, b)
      GroupData(
        title = if (second.title.nonEmpty) second.title else first.title,
        memberSet = SetDataOps.combine(a.memberSet, b.memberSet)
      )(ClockInt(second.clock, first.previousClock))
    }

    override def nextClock(current: Long): Long =
      System.currentTimeMillis max (current + 1L)

    override def diffFromClock(a: GroupData, from: Long): GroupData =
      a.copy(memberSet = SetDataOps.diffFromClock(a.memberSet, from))(ClockInt(a.clock, from))
    override def getRelations(data: GroupData): Set[String] = data.members.map(UserEntity(_).id)
  }

  case class GroupEntity(groupId: String) extends Entity {
    lazy val id: String = "group_" + groupId
    override val ops = GroupOps
  }
}

private class GroupActor(id: String, title: String, notifier: ActorRef)
  extends Actor with ActorDataMethods {
  import Group._
  import context.dispatcher
  private val log = LoggerFactory.getLogger(getClass)

  private implicit val timeout: Timeout = 3.seconds
  private val group = GroupEntity(id)
  protected val storage = new LocalDataStorage(log, ActorFacade(_, self), notifier.ask(_).mapTo[Unit])

  {
    implicit val cint = ClockInt(0L, System.currentTimeMillis)
    storage.addEntity(group)(GroupData(title, SetDataOps.zero[Long, Long]))
  }

  override def receive: Receive = handleDataMessage(group) orElse {
    case GetMembers => sender() ! storage.get(group).map(_.members.toSet).getOrElse(Set.empty)
    case AddMember(userId) =>
      storage.addRelation(UserEntity(userId))
      storage.updateEntity(group) { implicit clockInt => g =>
        g.copy(memberSet = g.memberSet.add(userId))
      } pipeTo sender()
  }
}