package ru.oseval.dnotifier

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.scalatest
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.easymock.EasyMockSugar
import ru.oseval.dnotifier.Data.{GetDifferenceFrom, RelatedDataUpdated}
import ru.oseval.dnotifier.Notifier.NotifyDataUpdated

class DataHolderSpec extends TestKit(ActorSystem("holderTest"))
  with ImplicitSender
  with FlatSpecLike
  with BeforeAndAfterAll
  with ScalaFutures
  with EasyMockSugar
  with scalatest.Matchers
  with Eventually {

  import ProductTestData._
  import WarehouseTestData._

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  private val notifier = TestProbe("notifier")

  behavior of "Data holder"

  it should "response on get difference from id" in {
    val productId = "Product 1"
    val productHolder = system.actorOf(productProps(productId, notifier.ref))
    productHolder ! Ping
    expectMsg(Pong)
    val product = ProductData("Product name", 1, System.currentTimeMillis)

    productHolder ! GetDifferenceFrom(productId, ProductOps.zero.clock)
    expectMsgType[ProductData] shouldEqual ProductOps.zero

    productHolder ! UpdateData(product)
    val dup = notifier.expectMsgType[NotifyDataUpdated]
    dup.data shouldEqual product
    notifier.lastSender ! ()
    expectMsgType[Unit]

    productHolder ! GetDifferenceFrom(productId, ProductOps.zero.clock)
    expectMsgType[ProductData] shouldEqual product
  }

  it should "send data update to notifier" in {
    val productId = "Product 1"
    val productHolder = system.actorOf(productProps(productId, notifier.ref))
    productHolder ! Ping
    expectMsg(Pong)
    val product = ProductData("Product name", 1, System.currentTimeMillis)
    productHolder ! UpdateData(product)
    val msg = notifier.expectMsgType[NotifyDataUpdated]
    msg.entityId shouldBe productId
    msg.data.clock shouldBe product.clock
  }

  it should "update related data" in {
    val productId = "Product 1"
    val warehouseId = "Warehouse 1"
    val warehouseHolder = system.actorOf(warehouseProps(warehouseId, notifier.ref))
    val product = ProductData("Product name", 1, System.currentTimeMillis)

    warehouseHolder ! GetDifferenceFrom(warehouseId, WarehouseOps.zero.clock)
    expectMsgType[WarehouseData] shouldEqual WarehouseOps.zero

    warehouseHolder ! AddProduct(productId)
    notifier.expectMsgType[NotifyDataUpdated]
    warehouseHolder ! ()

    warehouseHolder ! GetDifferenceFrom(warehouseId, WarehouseOps.zero.clock)
    expectMsgType[WarehouseData].products.values should contain(productId)

    warehouseHolder ! RelatedDataUpdated(warehouseId, productId, product)
    expectMsgType[Unit]
  }
}
