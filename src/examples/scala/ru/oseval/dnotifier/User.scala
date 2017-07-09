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

  object UserOps extends DataOps {
    override type DInner = UserData
    override val ordering: Ordering[String] = Data.timestampOrdering
    override val zero: UserData = UserData("", "0")
    override def combine(a: UserData, b: UserData): UserData =
      if (ordering.gt(a.clock, b.clock)) a else b

    override def diffFromClock(a: UserData, from: String): UserData = a
    override def getRelatedEntities(data: UserData): Set[String] = Set.empty
}
  class UserEntity(override val id: String,
                   override val notifyDataUpdated: (Notifier.NotifyDataUpdated) => Future[Unit],
                   initialData: UserData
                  ) extends AbstractEntity(initialData) {
    override val ops: UserOps.type = UserOps
  }

  case class UserFacade(id: String, holder: ActorRef) extends ActorFacade {
    override val ops: UserOps.type = UserOps
  }
}

private class UserActor(id: Long, name: String, notifier: ActorRef) extends Actor with ActorDataMethods {
  import context.dispatcher

  private implicit val timeout: Timeout = 3.seconds
  private val entityId = "user_" + id
  private val user = new UserEntity(entityId, notifier.ask(_).mapTo[Unit], UserData(name, System.currentTimeMillis.toString))

  notifier ! Register(UserFacade(entityId, self))

  override def receive: Receive = handleDataMessage(user) orElse {
    case ChangeName(n) => user.combine(UserData(n, System.currentTimeMillis.toString)) pipeTo sender()
  }
}