package ru.oseval.datahub

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import ru.oseval.datahub.User.{ChangeName, UserData, UserEntity}

import scala.concurrent.duration._

case class User(id: Long, name: String)

object User {
  def props(id: Long, name: String, notifier: ActorRef): Props =
    Props(classOf[UserActor], id, name, notifier)

  case class ChangeName(newName: String)

  case class UserData(name: String, clock: String) extends Data

  object UserOps extends DataOps[UserData] {
    override val ordering: Ordering[String] = Data.timestampOrdering
    override val zero: UserData = UserData("", "0")
    override def combine(a: UserData, b: UserData): UserData =
      if (ordering.gt(a.clock, b.clock)) a else b

    override def diffFromClock(a: UserData, from: String): UserData = a
    override def getRelations(data: UserData): Set[String] = Set.empty

    override def makeId(ownId: Any): String = "user_" + ownId
  }

  case class UserEntity(userId: Long) extends Entity {
    override type D = UserData
    override val ops = UserOps
    override val ownId: Any = id.toString
  }
}

private class UserActor(id: Long, name: String, notifier: ActorRef)
  extends Actor with ActorDataMethods with ActorLogging {
  import context.dispatcher

  private implicit val timeout: Timeout = 3.seconds
  private val user = UserEntity(id)
  protected val storage = new LocalDataStorage(log, ActorFacade(_, self), notifier.ask(_).mapTo[Unit])

  storage.addEntity(user)(UserData(name, System.currentTimeMillis.toString))

  override def receive: Receive = handleDataMessage(user) orElse {
    case ChangeName(n) =>
      storage.combine(user)(UserData(n, System.currentTimeMillis.toString)) pipeTo sender()
  }
}