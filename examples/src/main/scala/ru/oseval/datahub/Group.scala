package ru.oseval.datahub

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import org.slf4j.LoggerFactory
import ru.oseval.datahub.User.UserEntity
import ru.oseval.datahub.data.{Data, DataOps}

import scala.concurrent.duration._

case class Group(id: String, title: String, members: Set[Long])

object Group {
  def props(id: String, title: String, notifier: ActorRef): Props =
    Props(classOf[GroupActor], id, title, notifier)

  case class AddMember(userId: Long)
  case object GetMembers

  case class GroupData(title: String,
                       membersAdded: Map[Long, Long],
                       membersRemoved: Map[Long, Long],
                       clock: Long) extends Data {
    override type C = Long
    lazy val members = membersAdded.keySet
  }

  object GroupOps extends DataOps {
    override type D = GroupData
    override val ordering: Ordering[Long] = Ordering.Long
    override val zero: GroupData = GroupData("", Map.empty, Map.empty, 0L)
    override def combine(a: GroupData, b: GroupData): GroupData = {
      val (first, second) = if (ordering.gt(a.clock, b.clock)) (b, a) else (a, b)
      GroupData(
        title = if (second.title.nonEmpty) second.title else first.title,
        membersAdded = first.membersAdded ++ second.membersAdded,
        membersRemoved = first.membersRemoved ++ second.membersRemoved -- second.membersAdded.keySet,
        second.clock
      )
    }

    override def diffFromClock(a: GroupData, from: Long): GroupData =
      a.copy(
        membersAdded = a.membersAdded.filter { case (_, clock) => ordering.gt(clock, from) },
        membersRemoved = a.membersRemoved.filter { case (_, clock) => ordering.gt(clock, from) }
      )
    override def getRelations(data: GroupData): Set[String] = data.membersAdded.keySet.map("user_" + _)
  }

  case class GroupEntity(ownId: String) extends Entity {
    override type ID = String
    override val ops = GroupOps
    override def makeId(ownId: String): String = "group_" + ownId
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

  storage.addEntity(group)(GroupData(title, Map.empty, Map.empty, System.currentTimeMillis))

  override def receive: Receive = handleDataMessage(group) orElse {
    case GetMembers => sender() ! storage.get(group).map(_.members.toSet).getOrElse(Set.empty)
    case AddMember(userId) =>
      val newClock = System.currentTimeMillis
      storage.addRelation(UserEntity(userId))
      storage.combine(group)(
        GroupData("", membersAdded = Map(userId -> newClock), membersRemoved = Map.empty, newClock)
      ) pipeTo sender()
  }
}