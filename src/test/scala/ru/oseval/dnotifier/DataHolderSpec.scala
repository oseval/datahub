package ru.oseval.dnotifier

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.scalatest
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.easymock.EasyMockSugar
import ru.oseval.dnotifier.Data.{GetDifferenceFrom, RelatedDataUpdated}
import ru.oseval.dnotifier.Notifier.{NotifyDataUpdated, Register}

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
    val product = ProductEntity("1")
    val productHolder = system.actorOf(productProps(product.ownId, notifier.ref))

    productHolder ! Ping
    expectMsg(Pong)
    notifier.expectMsgType[Register]

    productHolder ! GetDifferenceFrom(product.id, product.ops.zero.clock)
    expectMsgType[ProductData] shouldEqual product.ops.zero

    val newProductData = ProductData("Product name", 1, System.currentTimeMillis)
    productHolder ! UpdateData(newProductData)
    notifier.expectMsgType[NotifyDataUpdated].data shouldEqual newProductData
    notifier.lastSender ! ()
    expectMsgType[Unit]

    productHolder ! GetDifferenceFrom(product.id, product.ops.zero.clock)
    expectMsgType[ProductData] shouldEqual newProductData
  }

  it should "send data update to notifier" in {
    val product = ProductEntity("2")
    val productHolder = system.actorOf(productProps(product.ownId, notifier.ref))

    productHolder ! Ping
    expectMsg(Pong)
    notifier.expectMsgType[Register]

    val productData = ProductData("Product name", 1, System.currentTimeMillis)
    productHolder ! UpdateData(productData)
    val msg = notifier.expectMsgType[NotifyDataUpdated]
    msg.entityId shouldBe product.id
    msg.data.clock shouldBe productData.clock
  }

  it should "update related data" in {
    val product = ProductEntity("3")
    val warehouse = WarehouseEntity("1")
    val warehouseHolder = system.actorOf(warehouseProps(warehouse.ownId, notifier.ref))

    notifier.expectMsgType[Register]

    warehouseHolder ! GetDifferenceFrom(warehouse.id, WarehouseOps.zero.clock)
    expectMsgType[WarehouseData] shouldEqual WarehouseOps.zero

    warehouseHolder ! AddProduct(product.ownId)
    notifier.expectMsgType[NotifyDataUpdated]
    warehouseHolder ! ()

    warehouseHolder ! GetDifferenceFrom(warehouse.id, WarehouseOps.zero.clock)
    expectMsgType[WarehouseData].products.values should contain(product.id)

    val productData = ProductData("Product name", 1, System.currentTimeMillis)
    warehouseHolder ! RelatedDataUpdated(warehouse.id, product.id, productData)
    expectMsgType[Unit]
  }
}
