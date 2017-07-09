package ru.oseval.dnotifier

import akka.util.Timeout
import akka.pattern.ask
import org.scalatest
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.easymock.EasyMockSugar
import ru.oseval.dnotifier.Notifier._

import scala.concurrent.duration._
import ProductTestData._
import WarehouseTestData._
import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import ru.oseval.dnotifier.Data.{GetDifferenceFrom, RelatedDataUpdated}

class NotifierSpec extends TestKit(ActorSystem("notifierTest"))
  with ImplicitSender
  with FlatSpecLike
  with BeforeAndAfterAll
  with ScalaFutures
  with EasyMockSugar
  with scalatest.Matchers
  with Eventually {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  private implicit val timeout: Timeout = 15.seconds

  def storage = new MemoryStorage

  behavior of "Notifier"

  private implicit val ec = system.dispatcher

  it should "register data entities" in {
    val holderProbe = TestProbe("holder")
    val notifier = system.actorOf(Notifier.props(storage))

    val entity = ProductFacade("Product single", holderProbe.ref)
    notifier ! Register(entity, Map.empty)
    expectMsgType[Unit]
  }

  it should "subscribe on related data entities" in {
    val notifier = system.actorOf(Notifier.props(storage))

    val productHolderProbe = TestProbe("productHolder")
    val product = ProductFacade("Product1", productHolderProbe.ref)
    val productData = ProductOps.zero

    // cache of product data
    val warehouseHolderProbe = TestProbe("warehouseHolder")
    val dependOnStreams = Map(product.id → productData.clock)
    val warehouse = WarehouseFacade("Warehouse", warehouseHolderProbe.ref)

    // Register product
    notifier.ask(Register(product, Map.empty)).futureValue

    // Product entity data is updated
    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
    notifier ! NotifyDataUpdated(product.id, newProductData)
    expectMsgType[Unit]

    // Register warehouse which depends on product, get updates from it
    val warehouseRegisterRes = notifier.ask(Register(warehouse, dependOnStreams))

    val res = productHolderProbe.expectMsgType[GetDifferenceFrom]
    res.dataClock shouldEqual productData.clock
    productHolderProbe.lastSender ! newProductData

    val res2 = warehouseHolderProbe.expectMsgType[RelatedDataUpdated]
    res2.data.clock shouldEqual newProductData.clock
    warehouseHolderProbe.lastSender ! ()

    warehouseRegisterRes.futureValue
  }

  it should "receive updates from related entities" in {
    val notifier = system.actorOf(Notifier.props(storage))

    val productHolderProbe = TestProbe("productHolder")
    val product = ProductFacade("Product1", productHolderProbe.ref)
    val productData = ProductOps.zero

    // cache of product data
    val warehouseHolderProbe = TestProbe("warehouseHolder")
    val dependOnStreams = Map(product.id → productData.clock)
    val warehouse = WarehouseFacade("Warehouse", warehouseHolderProbe.ref)

    // Register product
    notifier ! Register(product, Map.empty)
    expectMsgType[Unit]

    // Register warehouse which depends on product, get updates from it
    notifier ! Register(warehouse, dependOnStreams)
    expectMsgType[Unit]

    // Product entity data is updated
    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
    val productDataUpdateRes = notifier.ask(NotifyDataUpdated(product.id, newProductData))

    val res = warehouseHolderProbe.expectMsgType[RelatedDataUpdated]
    res.data.clock shouldEqual newProductData.clock
    warehouseHolderProbe.lastSender ! ()

    productDataUpdateRes.futureValue
  }

  it should "subscribe entity to new related entities" in {
    val notifier = system.actorOf(Notifier.props(storage))

    val productHolderProbe = TestProbe("productHolder")
    val productId = "Product1"
    val product = ProductFacade(productId, productHolderProbe.ref)
    val productData = ProductOps.zero

    // cache of product data
    val warehouseHolderProbe = TestProbe("warehouseHolder")
    val warehouseId = "Warehouse 1"
    val warehouse = WarehouseFacade(warehouseId, warehouseHolderProbe.ref)

    // Register product
    notifier.ask(Register(product, Map.empty)).futureValue

    // Register warehouse
    notifier.ask(Register(warehouse, Map.empty)).futureValue

    // Send update with new related entity
    val newWarehouseData = WarehouseData(Map(System.currentTimeMillis.toString → productId))
    notifier.ask(NotifyDataUpdated(warehouseId, newWarehouseData)).futureValue

    // Product entity data is updated
    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
    notifier.ask(NotifyDataUpdated(productId, newProductData)).futureValue

    val res = warehouseHolderProbe.expectMsgType[RelatedDataUpdated]
    res.data.clock shouldEqual newProductData.clock
    warehouseHolderProbe.lastSender ! ()
  }
}