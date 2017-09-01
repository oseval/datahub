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

    val facade = ActorFacade(ProductEntity("Product1"), holderProbe.ref)
    notifier ! Register(facade, ProductOps.zero.clock, Map.empty)
    expectMsgType[Unit]
  }

  it should "subscribe on related data entities" in {
    val notifier = system.actorOf(Notifier.props(storage))

    val productHolderProbe = TestProbe("productHolder")
    val product = ProductEntity("Product1")
    val productFacade = ActorFacade(product, productHolderProbe.ref)
    val productData = ProductOps.zero

    // cache of product data
    val warehouseHolderProbe = TestProbe("warehouseHolder")
    val warehouse = WarehouseEntity("Warehouse1")
    val warehouseFacade = ActorFacade(warehouse, warehouseHolderProbe.ref)
    val warehouseData = WarehouseData(Map(System.currentTimeMillis.toString -> product.id))

    // Register product
    notifier.ask(Register(productFacade, productData.clock, Map.empty)).futureValue

    // Product entity data is updated
    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
    notifier ! NotifyDataUpdated(product.id, newProductData)
    expectMsgType[Unit]

    // Register warehouse which depends on product, get updates from it
    val warehouseRegisterRes = notifier.ask(
      Register(warehouseFacade, warehouseData.clock, Map(product.id -> ProductOps.zero.clock))
    )

    productHolderProbe.expectMsgType[GetDifferenceFrom].dataClock shouldEqual productData.clock
    productHolderProbe.lastSender ! newProductData

    val res2 = warehouseHolderProbe.expectMsgType[RelatedDataUpdated]
    res2.data.clock shouldEqual newProductData.clock
    warehouseHolderProbe.lastSender ! ()

    warehouseRegisterRes.futureValue
  }

  it should "receive updates from related entities" in {
    val notifier = system.actorOf(Notifier.props(storage))

    val productHolderProbe = TestProbe("productHolder")
    val product = ProductEntity("Product1")
    val productData = ProductOps.zero

    // cache of product data
    val warehouseHolderProbe = TestProbe("warehouseHolder")
    val warehouseData = WarehouseData(Map(System.currentTimeMillis.toString -> product.id))
    val warehouseFacade = ActorFacade(WarehouseEntity("Warehouse1"), warehouseHolderProbe.ref)

    // Register product
    notifier ! Register(ActorFacade(product, productHolderProbe.ref), productData.clock, Map.empty)
    expectMsgType[Unit]

    // Register warehouse which depends on product, get updates from it
    notifier ! Register(warehouseFacade, warehouseData.clock, Map(product.id → productData.clock))
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
    val product = ProductEntity("1")

    // cache of product data
    val warehouseHolderProbe = TestProbe("warehouseHolder")
    val warehouse = WarehouseEntity("1")
    val warehouseFacade = ActorFacade(warehouse, warehouseHolderProbe.ref)

    // Register product
    notifier.ask(Register(ActorFacade(product, productHolderProbe.ref), ProductOps.zero.clock, Map.empty)).futureValue

    // Register warehouse
    notifier.ask(Register(warehouseFacade, WarehouseOps.zero.clock, Map.empty)).futureValue

    // Send update with new related entity
    val newWarehouseData = WarehouseData(Map(System.currentTimeMillis.toString → product.id))
    notifier.ask(NotifyDataUpdated(warehouse.id, newWarehouseData)).futureValue

    // Product entity data is updated
    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
    notifier.ask(NotifyDataUpdated(product.id, newProductData)).futureValue

    warehouseHolderProbe.expectMsgType[RelatedDataUpdated].data.clock shouldEqual newProductData.clock
    warehouseHolderProbe.lastSender ! ()
  }
}