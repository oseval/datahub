package ru.oseval.datahub

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import org.slf4j.LoggerFactory
import ru.oseval.datahub.User.{UserEntity, UserOps}
import ru.oseval.datahub.data.{Data, DataOps, SetData, SetDataOps}

import scala.concurrent.duration._

case class Group(id: String, title: String, members: Set[Long])

object Group {
  def props(id: String, title: String, notifier: ActorRef): Props =
    Props(classOf[GroupActor], id, title, notifier)

  case class AddMember(userId: Long)
  case object GetMembers

  case class GroupData(title: String,
                       memberSet: SetData[Long, Long],
                       clock: Long) extends Data {
    override type C = Long
    lazy val members = memberSet.elements.toSet
  }

  object GroupOps extends DataOps {
    override type D = GroupData
    override val ordering: Ordering[Long] = Ordering.Long
    override val zero: GroupData = GroupData("", SetDataOps.zero(0L, 0L), 0L)
    override def makeId(ownId: Any): String = "group_" + ownId
    override def combine(a: GroupData, b: GroupData): GroupData = {
      val (first, second) = if (ordering.gt(a.clock, b.clock)) (b, a) else (a, b)
      GroupData(
        title = if (second.title.nonEmpty) second.title else first.title,
        memberSet = SetDataOps.combine(a.memberSet, b.memberSet),
        second.clock
      )
    }

    override def diffFromClock(a: GroupData, from: Long): GroupData =
      a.copy(memberSet = SetDataOps.diffFromClock(a.memberSet, from))
    override def getRelations(data: GroupData): Set[String] = data.members.map(UserEntity(_).id)
  }

  case class GroupEntity(ownId: String) extends Entity {
    override type ID = String
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
    val currentTime = System.currentTimeMillis
    storage.addEntity(group)(GroupData(title, SetDataOps.zero(0L, currentTime), currentTime))
  }

  override def receive: Receive = handleDataMessage(group) orElse {
    case GetMembers => sender() ! storage.get(group).map(_.members.toSet).getOrElse(Set.empty)
    case AddMember(userId) =>
      val newClock = System.currentTimeMillis
      storage.addRelation(UserEntity(userId))
      storage.combine(group)(
        GroupData("", memberSet = SetDataOps.zero(0L, 0L).add(userId, newClock), newClock)
      ) pipeTo sender()
  }
}