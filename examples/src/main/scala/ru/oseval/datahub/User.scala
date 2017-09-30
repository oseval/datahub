package ru.oseval.datahub

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import org.slf4j.LoggerFactory
import ru.oseval.datahub.User.{ChangeName, UserData, UserEntity}
import ru.oseval.datahub.data.{Data, DataOps}

import scala.concurrent.duration._

case class User(id: Long, name: String)

object User {
  def props(id: Long, name: String, notifier: ActorRef): Props =
    Props(classOf[UserActor], id, name, notifier)

  case class ChangeName(newName: String)

  case class UserData(name: String, clock: Long) extends Data {
    override type C = Long
  }

  object UserOps extends DataOps {
    override type D = UserData
    override val ordering: Ordering[Long] = Ordering.Long
    override val zero: UserData = UserData("", 0L)

    override def combine(a: UserData, b: UserData): UserData =
      if (ordering.gt(a.clock, b.clock)) a else b

    override def diffFromClock(a: UserData, from: Long): UserData = a
    override def getRelations(data: UserData): Set[String] = Set.empty
    override def makeId(ownId: Any): String = "user_" + ownId

    override def nextClock(current: Long): Long =
      System.currentTimeMillis max (current + 1L)
  }

  case class UserEntity(ownId: Long) extends Entity {
    override type ID = Long
    override val ops = UserOps
  }
}

private class UserActor(id: Long, name: String, notifier: ActorRef)
  extends Actor with ActorDataMethods {
  import context.dispatcher

  private val log = LoggerFactory.getLogger(getClass)

  private implicit val timeout: Timeout = 3.seconds
  private val user = UserEntity(id)
  protected val storage = new LocalDataStorage(log, ActorFacade(_, self), notifier.ask(_).mapTo[Unit])

  storage.addEntity(user)(UserData(name, System.currentTimeMillis))

  override def receive: Receive = handleDataMessage(user) orElse {
    case ChangeName(n) =>
      storage.updateEntity(user)(cint => _.copy(name = n, clock = cint.cur)) pipeTo sender()
  }
}