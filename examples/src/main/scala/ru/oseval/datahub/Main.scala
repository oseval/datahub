package ru.oseval.datahub

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._

object Main extends App {
  implicit val timeout: Timeout = 3.seconds

  val system = ActorSystem("example")
  val storage = new MemoryStorage
  val notifier = system.actorOf(ActorDatahub.props(storage))

  val user1 = system.actorOf(User.props(1, "User 1", notifier))
  val user2 = system.actorOf(User.props(2, "User 2", notifier))
  val group = system.actorOf(Group.props("groupId", "Test group", notifier))

  import Group._
  import User._

  val emptyMembers = Await.result(group.ask(GetMembers).mapTo[Set[User]], timeout.duration)
  // Set()

  Await.result(group.ask(AddMember(1)), timeout.duration)
  Await.result(group.ask(AddMember(2)), timeout.duration)

  val members = Await.result(group.ask(GetMembers).mapTo[Set[User]], timeout.duration)
  // Set(User(1), User(2))

  Await.result(user1.ask(ChangeName("User 1 new name")), timeout.duration)

  val members2 = Await.result(group.ask(GetMembers).mapTo[Set[User]], timeout.duration)
  // Set(User(1, User 1 new name), User(2))
}
