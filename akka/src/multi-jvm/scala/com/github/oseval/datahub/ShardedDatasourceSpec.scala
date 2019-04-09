package com.github.oseval.datahub

import akka.actor.{ActorRef, ActorSystem}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec, MultiNodeSpecCallbacks}
import akka.testkit.{ImplicitSender, TestProbe}
import com.github.oseval.datahub.cluster.ShardedDataSource
import com.github.oseval.datahub.domain.GroupRegion.{SetProbe, Updated}
import com.github.oseval.datahub.domain.UserRegion.{UpdateName, UserDataOps, UserEntity}
import com.github.oseval.datahub.domain.{GroupRegion, UserRegion}
import com.typesafe.config.ConfigFactory
import org.mockito.Mockito
import org.mockito.verification.VerificationWithTimeout
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.duration._

object MultiNodeSpecConfig extends MultiNodeConfig {
  testTransport(true)
  debugConfig(true)

  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")
  val node4 = role("node4")
  val node5 = role("node5")
  val node6 = role("node6")

  nodeConfig(node1)(ConfigFactory.parseString("timeshift = 1"))
  nodeConfig(node2)(ConfigFactory.parseString("timeshift = 2"))
  nodeConfig(node3)(ConfigFactory.parseString("timeshift = 3"))
  nodeConfig(node4)(ConfigFactory.parseString("timeshift = 4"))
  nodeConfig(node5)(ConfigFactory.parseString("timeshift = 5"))
  nodeConfig(node6)(ConfigFactory.parseString("timeshift = 6"))

  val systemName = "ShardedDataSourceSpec"

  commonConfig(ConfigFactory.parseString(
    s"""
       |akka {
       |  loglevel = "DEBUG"
       |  log-dead-letters = 10
       |  log-dead-letters-during-shutdown = off
       |  actor.provider: "cluster"
       |  cluster {
       |    log-info = on
       |    min-nr-of-members = 6
       |    failure-detector {
       |      heartbeat-interval = 1000 millis
       |      threshold = 0.5
       |      acceptable-heartbeat-pause = 3000 millis
       |    }
       |  }
       |  remote {
       |    log-remote-lifecycle-events = on
       |    watch-failure-detector.acceptable-heartbeat-pause = 5000 millis
       |  }
       |
       |  testconductor.barrier-timeout = 10s
       |}
     """.stripMargin
  ))
}

class ShardedDatasourceSpecMultiJvmNode1 extends ShardedDatasourceSpec
class ShardedDatasourceSpecMultiJvmNode2 extends ShardedDatasourceSpec
class ShardedDatasourceSpecMultiJvmNode3 extends ShardedDatasourceSpec
class ShardedDatasourceSpecMultiJvmNode4 extends ShardedDatasourceSpec
class ShardedDatasourceSpecMultiJvmNode5 extends ShardedDatasourceSpec
class ShardedDatasourceSpecMultiJvmNode6 extends ShardedDatasourceSpec

class ShardedDatasourceSpec extends MultiNodeSpec(MultiNodeSpecConfig, { cfg ⇒
  // timeshift for predictable system uptime and the leader election process to write correct test
  Thread.sleep(cfg.getLong("timeshift") * 1000)
  ActorSystem("ShardedDatasourceSpec", cfg)
})
  with MultiNodeSpecCallbacks
  with ImplicitSender
  with MockitoSugar
  with FlatSpecLike
  with Matchers
  with Eventually
  with BeforeAndAfterAll {

  import MultiNodeSpecConfig._

  override def beforeAll() = multiNodeSpecBeforeAll()
  override def afterAll() = multiNodeSpecAfterAll()

  def initialParticipants = roles.size

  private val timeout: FiniteDuration = 10.seconds
  private val tm: VerificationWithTimeout = Mockito.timeout(timeout.toMillis)

  private def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      Cluster(system) join node(to).address

      fishForMessage(timeout, s"Member up: " + node(from).address) {
        case MemberJoined(member) ⇒ member.address == node(from).address
        case e ⇒ false
      }
    }

    enterBarrier(from.name + "-joined")
  }

  private def initDatahub(implicit system: ActorSystem) = {
    val dh = new AsyncDatahub()(system.dispatcher)
    val userRegion = UserRegion.start(system)
    val ds = new ShardedDataSource(userRegion, UserDataOps, dh.dataUpdated) {}

    dh.register(ds)

    dh -> userRegion
  }

  testTransport(false)
  debugConfig(true)

  var dh: Datahub = _
  var userRegion: ActorRef = _

  behavior of "Sharding data source"

  it should "init sharded entities" in {
    enterBarrier("start")

    Cluster(system).subscribe(testActor, InitialStateAsEvents, classOf[ClusterDomainEvent])

    runOn(node1) {
      Cluster(system) joinSeedNodes List(node(node1).address)

      // does not up while cluster size < min-nr-of-members
      fishForMessage(5.seconds, "Leader up") {
        case MemberJoined(member) ⇒ member.address == node(node1).address
        case _ ⇒ false
      }
    }

    enterBarrier("Leader joined")

    join(node2, node1)
    join(node3, node1)
    join(node4, node1)
    join(node5, node1)
    join(node6, node1)

    enterBarrier("All joined")

    runOn(node1) {
      Cluster(system) joinSeedNodes List(node(node1).address)

      // does not up while cluster size < min-nr-of-members
      fishForMessage(5.seconds, "Leader up") {
        case MemberUp(member) ⇒ member.address == node(node1).address
        case _ ⇒ false
      }
    }

    enterBarrier("Leader Up")

    val res = initDatahub(system)
    dh = res._1
    userRegion = res._2

    enterBarrier("Datahub started")
  }

  it should "subscribe on sharded relation" in {
    val groupRegion = GroupRegion.start(system, dh)
    enterBarrier("Group region started")

    runOn(node2) {
      val probe = TestProbe()
      groupRegion ! SetProbe("testGroup", probe.ref)
      probe.expectMsgType[Unit](10.seconds)

      val msgs = probe.receiveN(3, 10.seconds)

      msgs.collect { case Updated(entityId, data) =>
        entityId -> data.data.get.name
      } should contain theSameElementsAs Seq("user_1" -> "Name_1", "user_2" -> "Name_2", "user_3" -> "Name_3")
    }

    enterBarrier("Passed subscribe on sharded relation")
  }

  it should "send updates to subscribers" in {
    val groupRegion = GroupRegion.start(system, dh)
    enterBarrier("Group region started")

    runOn(node3) {
      val probe = TestProbe()
      val groupRegion = GroupRegion.start(system, dh)

      groupRegion ! SetProbe("testGroup", probe.ref)
      probe.expectMsgType[Unit]
      probe.expectNoMessage(1.seconds)

      userRegion ! UpdateName(UserEntity(3).id, "Petya")

      val Updated(entityId, data) = probe.expectMsgType[Updated](10.seconds)
      entityId shouldBe "user_3"
      data.data.get.name shouldBe "Petya"
    }

    enterBarrier("Passed subscribe on sharded relation")
  }
}
