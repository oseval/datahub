package ru.oseval.datahub

import akka.util.Timeout
import akka.pattern.ask
import org.scalatest
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import ru.oseval.datahub.Datahub._

import scala.concurrent.duration._
import ProductTestData._
import WarehouseTestData._
import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import ActorFacadeMessages._
import ru.oseval.datahub.data.{ALOData, ClockInt}

class DatahubSpec extends TestKit(ActorSystem("notifierTest"))
  with ImplicitSender
  with FlatSpecLike
  with BeforeAndAfterAll
  with ScalaFutures
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
    val notifier = system.actorOf(ActorDatahub.props(storage))

    val facade = ActorFacade(ProductEntity(1), holderProbe.ref)
    notifier ! Register(facade, Map.empty)(facade.entity.ops.zero.clock)
    expectMsgType[Unit]
  }

  it should "subscribe on related data entities" in {
    val notifier = system.actorOf(ActorDatahub.props(storage))

    val productHolderProbe = TestProbe("productHolder")
    val product = ProductEntity(2)
    val productFacade = ActorFacade(product, productHolderProbe.ref)
    val productData = productFacade.entity.ops.zero

    // cache of product data
    val warehouseHolderProbe = TestProbe("warehouseHolder")
    val warehouse = WarehouseEntity("Warehouse1")
    val warehouseFacade = ActorFacade(warehouse, warehouseHolderProbe.ref)
    val warehouseData = ALOData(product.id)(ClockInt(System.currentTimeMillis, 0L))

    // Register product
    notifier.ask(Register(productFacade, Map.empty)(productData.clock)).futureValue

    // Product entity data is updated
    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
    notifier ! DataUpdated(product.id, newProductData)
    expectMsgType[Unit]

    // Register warehouse which depends on product, get updates from it
    val warehouseRegisterRes = notifier.ask(
      Register(warehouseFacade, Map(product.id -> ProductOps.zero.clock))(
        warehouseData.clock.asInstanceOf[warehouseFacade.entity.ops.D#C]
      )
    )

    productHolderProbe.expectMsgType[GetDifferenceFrom].dataClock shouldEqual productData.clock
    productHolderProbe.lastSender ! newProductData

    val res2 = warehouseHolderProbe.expectMsgType[RelatedDataUpdated]
    res2.data.clock shouldEqual newProductData.clock
    warehouseHolderProbe.lastSender ! ()

    warehouseRegisterRes.futureValue
  }

  it should "receive updates from related entities" in {
    val notifier = system.actorOf(ActorDatahub.props(storage))

    val productHolderProbe = TestProbe("productHolder")
    val product = ProductEntity(3)
    val productData = ProductOps.zero
    val productFacade = ActorFacade(product, productHolderProbe.ref)

    // cache of product data
    val warehouseHolderProbe = TestProbe("warehouseHolder")
    val warehouseData = ALOData(product.id)(ClockInt(System.currentTimeMillis, 0L))
    val warehouseFacade = ActorFacade(WarehouseEntity("Warehouse1"), warehouseHolderProbe.ref)

    // Register product
    notifier ! Register(productFacade, Map.empty)(
      productData.clock.asInstanceOf[productFacade.entity.ops.D#C]
    )
    expectMsgType[Unit]

    // Register warehouse which depends on product, get updates from it
    notifier ! Register(warehouseFacade, Map(product.id → productData.clock))(
      warehouseData.clock.asInstanceOf[warehouseFacade.entity.ops.D#C]
    )
    expectMsgType[Unit]

    // Product entity data is updated
    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
    val productDataUpdateRes = notifier.ask(DataUpdated(product.id, newProductData))

    val res = warehouseHolderProbe.expectMsgType[RelatedDataUpdated]
    res.data.clock shouldEqual newProductData.clock
    warehouseHolderProbe.lastSender ! ()

    productDataUpdateRes.futureValue
  }

  it should "subscribe entity to new related entities" in {
    val notifier = system.actorOf(ActorDatahub.props(storage))

    val productHolderProbe = TestProbe("productHolder")
    val product = ProductEntity(4)
    val productFacade = ActorFacade(product, productHolderProbe.ref)

    // cache of product data
    val warehouseHolderProbe = TestProbe("warehouseHolder")
    val warehouse = WarehouseEntity("1")
    val warehouseFacade = ActorFacade(warehouse, warehouseHolderProbe.ref)

    // Register product
    notifier.ask(Register(productFacade, Map.empty)(
      ProductOps.zero.clock.asInstanceOf[productFacade.entity.ops.D#C]
    )).futureValue

    // Register warehouse
    notifier.ask(Register(warehouseFacade, Map.empty)(
      WarehouseOps.zero.clock.asInstanceOf[warehouseFacade.entity.ops.D#C]
    )).futureValue

    // Send update with new related entity
    val newWarehouseData = ALOData(product.id)(ClockInt(System.currentTimeMillis, 0L))
    notifier.ask(DataUpdated(warehouse.id, newWarehouseData)).futureValue

    // Product entity data is updated
    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
    notifier.ask(DataUpdated(product.id, newProductData)).futureValue

    warehouseHolderProbe.expectMsgType[RelatedDataUpdated].data.clock shouldEqual newProductData.clock
    warehouseHolderProbe.lastSender ! ()
  }
}