package ru.oseval.dnotifier

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import ru.oseval.dnotifier.Notifier.Register
import ru.oseval.dnotifier.User.{ChangeName, UserData, UserEntity, UserFacade}

import scala.concurrent.Future
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
  }

  case class UserEntity(id: String) extends Entity {
    override val ops = UserOps
  }
}

private class UserActor(id: Long, name: String, notifier: ActorRef) extends Actor with ActorDataMethods {
  import context.dispatcher

  private implicit val timeout: Timeout = 3.seconds
  private val user = UserEntity("user_" + id)
  private val storage = new LocalDataStorage(ActorFacade(_, self), notifier.ask(_).mapTo[Unit])

  storage.addEntity(user)
  storage.combine(user, UserData(name, System.currentTimeMillis.toString))

  override def receive: Receive = handleDataMessage(user) orElse {
    case ChangeName(n) => user.combine(UserData(n, System.currentTimeMillis.toString)) pipeTo sender()
  }
}