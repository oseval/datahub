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

  object GroupOps extends DataOps {
    override type DInner = GroupData
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
  }
  class GroupEntity(override val id: String,
                    override val notifyDataUpdated: (Notifier.NotifyDataUpdated) => Future[Unit],
                    initialData: GroupData
                   ) extends AbstractEntity(initialData) {
    override val ops = GroupOps

    private val users = mutable.Map.empty[Long, UserData]

    override def relatedDataUpdated(relatedId: String, related: Data): Unit = related match {
      case d: UserData =>
        val userId = relatedId.split('_')(1).toLong
        if (data.membersAdded.isDefinedAt(userId)) {
          users.get(userId) match {
            case None => users.update(userId, d)
            case Some(old) => users.update(userId, UserOps.combine(old, d))
          }
        }
      case _ =>
    }

    override def combine(otherData: GroupData): Future[Unit] = {
      val res = super.combine(otherData)
      users --= data.membersRemoved.keySet
      res
    }

    def addUser(userId: Long): Unit = users.update(userId, UserOps.zero)
    def getUsers: Set[User] = users.map { case (userId, d) => User(userId, d.name) }.toSet
  }

  case class GroupFacade(id: String, holder: ActorRef) extends ActorFacade {
    override val ops: GroupOps.type = GroupOps
  }
}

private class GroupActor(id: String, title: String, notifier: ActorRef) extends Actor with ActorDataMethods {
  import Group._
  import context.dispatcher

  private implicit val timeout: Timeout = 3.seconds
  private val entityId = "group_" + id
  private val group = new GroupEntity(
    entityId,
    notifier.ask(_).mapTo[Unit],
    GroupData(title, Map.empty, Map.empty, System.currentTimeMillis.toString)
  )

  notifier ! Register(GroupFacade(entityId, self))

  override def receive: Receive = handleDataMessage(group) orElse {
    case GetMembers => sender() ! group.getUsers
    case AddMember(userId) =>
      val newClock = System.currentTimeMillis.toString
      group.addUser(userId)
      group.combine(group.data.copy(membersAdded = Map(userId -> newClock))) pipeTo sender()
  }
}