package ru.oseval.datahub

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.scalatest
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import ru.oseval.datahub.Datahub.{DataUpdated, Register}
import ActorFacadeMessages._
import ru.oseval.datahub.data.ALOData

class DataHolderSpec extends TestKit(ActorSystem("holderTest"))
  with ImplicitSender
  with FlatSpecLike
  with BeforeAndAfterAll
  with ScalaFutures
  with scalatest.Matchers
  with Eventually {

  import ProductTestData._
  import ActorProductTestData._
  import WarehouseTestData._
  import ActorWarehouseTestData._

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  private val notifier = TestProbe("notifier")

  behavior of "Data holder"

  it should "response on get difference from id" in {
    val product = ProductEntity(1)
    val productHolder = system.actorOf(productProps(product.productId, notifier.ref))

    productHolder ! Ping
    expectMsg(Pong)
    notifier.expectMsgType[Register[_]]

    productHolder ! GetDifferenceFrom(product.id, product.ops.zero.clock)
    expectMsgType[ProductData] shouldEqual product.ops.zero

    val newProductData = ProductData("Product name", 1, System.currentTimeMillis)
    productHolder ! UpdateData(newProductData)
    notifier.expectMsgType[DataUpdated].data shouldEqual newProductData
    notifier.lastSender ! ()
    expectMsgType[Unit]

    productHolder ! GetDifferenceFrom(product.id, product.ops.zero.clock)
    expectMsgType[ProductData] shouldEqual newProductData
  }

  it should "send data update to notifier" in {
    val product = ProductEntity(2)
    val productHolder = system.actorOf(productProps(product.productId, notifier.ref))

    productHolder ! Ping
    expectMsg(Pong)
    notifier.expectMsgType[Register[_]]

    val productData = ProductData("Product name", 1, System.currentTimeMillis)
    productHolder ! UpdateData(productData)
    val msg = notifier.expectMsgType[DataUpdated]
    msg.entityId shouldBe product.id
    msg.data.clock shouldBe productData.clock
  }

  it should "update related data" in {
    val product = ProductEntity(3)
    val warehouse = WarehouseEntity("1")
    val warehouseHolder = system.actorOf(warehouseProps(warehouse.warehouseId, notifier.ref))

    notifier.expectMsgType[Register[_]]

    warehouseHolder ! GetDifferenceFrom(warehouse.id, WarehouseOps.zero.clock)
    expectMsgType[ALOData[String]] shouldEqual WarehouseOps.zero

    warehouseHolder ! AddProduct(product.productId)
    notifier.expectMsgType[DataUpdated]
    warehouseHolder ! ()

    warehouseHolder ! GetDifferenceFrom(warehouse.id, WarehouseOps.zero.clock)
    expectMsgType[ALOData[String]].elements should contain(product.id)

    val productData = ProductData("Product name", 1, System.currentTimeMillis)
    warehouseHolder ! RelatedDataUpdated(warehouse.id, product.id, productData)
    expectMsgType[Unit]
  }
}
