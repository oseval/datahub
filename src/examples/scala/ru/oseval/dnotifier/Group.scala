package ru.oseval.dnotifier

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import ru.oseval.dnotifier.Notifier.Register
import ru.oseval.dnotifier.User.{UserData, UserOps}

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

case class Group(id: String, title: String, members: Set[Long])

object Group {
  def props(id: String, title: String, notifier: ActorRef): Props =
    Props(classOf[GroupActor], id, title, notifier)

  case class AddMember(userId: Long)
  case object GetMembers

  case class GroupData(title: String,
                       membersAdded: Map[Long, String],
                       membersRemoved: Map[Long, String],
                       clock: String) extends Data

  object GroupOps extends DataOps[GroupData] {
    override val ordering: Ordering[String] = Data.timestampOrdering
    override val zero: GroupData = GroupData("", Map.empty, Map.empty, "0")
    override def combine(a: GroupData, b: GroupData): GroupData = {
      val (first, second) = if (ordering.gt(a.clock, b.clock)) (b, a) else (a, b)
      GroupData(
        title = first.title,
        membersAdded = first.membersAdded ++ second.membersAdded,
        membersRemoved = first.membersRemoved ++ second.membersRemoved -- second.membersAdded.keySet,
        second.clock
      )
    }

    override def diffFromClock(a: GroupData, from: String): GroupData =
      a.copy(
        membersAdded = a.membersAdded.filter { case (_, clock) => ordering.gt(clock, from) },
        membersRemoved = a.membersRemoved.filter { case (_, clock) => ordering.gt(clock, from) }
      )
    override def getRelatedEntities(data: GroupData): Set[String] = data.membersAdded.keySet.map("user_" + _)

    override def makeId(ownId: Any): String = "group_" + ownId
  }

  case class GroupEntity(groupId: String) extends Entity[UserData] {
    override val ops = GroupOps
  }
}

private class GroupActor(id: String, title: String, notifier: ActorRef) extends Actor with ActorDataMethods {
  import Group._
  import context.dispatcher

  private implicit val timeout: Timeout = 3.seconds
  private val group = GroupEntity(id)
  protected val storage = new LocalDataStorage(ActorFacade(_, self), notifier.ask(_).mapTo[Unit])

  storage.addEntity(group)

  override def receive: Receive = handleDataMessage(group) orElse {
    case GetMembers => sender() ! storage(entity)
    case AddMember(userId) =>
      val newClock = System.currentTimeMillis.toString
      storage.addRelation(UserEntity(userId))
      storage.combine(group, GroupData(membersAdded = Map(userId -> newClock))) pipeTo sender()
  }
}