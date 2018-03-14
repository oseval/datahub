package ru.oseval.datahub

import akka.util.Timeout
import akka.pattern.ask
import org.scalatest
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._
import ProductTestData._
import WarehouseTestData._
import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import ActorFacadeMessages._
import ru.oseval.datahub.AkkaDatahub._
import ru.oseval.datahub.data.{ALOData, ClockInt}

class AsyncDatahubSpec extends TestKit(ActorSystem("notifierTest"))
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

  def datahub = AkkaDatahub(new MemoryStorage)(system, ec)

  behavior of "Notifier"

  private implicit val ec = system.dispatcher

  it should "register data entities" in {
    val holderProbe = TestProbe("holder")
    val hub = datahub

    val facade = ActorFacade(ProductEntity(1), holderProbe.ref)
    hub.register(facade)(facade.entity.ops.zero.clock, Map.empty, Set.empty)
    expectMsgType[Unit]
  }

  it should "subscribe on related data entities" in {
    val hub = datahub

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
    hub.register(productFacade)(productData.clock, Map.empty, Set.empty).futureValue

    // Product entity data is updated
    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
    hub.dataUpdated(product, Set.empty)(newProductData)
    expectMsgType[Unit]

    // Register warehouse which depends on product, get updates from it
    val warehouseRegisterRes = hub.register(warehouseFacade)(
      warehouseData.clock.asInstanceOf[warehouseFacade.entity.ops.D#C],
      Map(product -> ProductOps.zero.clock),
      Set.empty
    )

    productHolderProbe.expectMsgType[GetDifferenceFrom].dataClock shouldEqual productData.clock
    productHolderProbe.lastSender ! newProductData

    val res2 = warehouseHolderProbe.expectMsgType[RelatedDataUpdated]
    res2.data.clock shouldEqual newProductData.clock
    warehouseHolderProbe.lastSender ! ()

    warehouseRegisterRes.futureValue
  }

  it should "receive updates from related entities" in {
    val hub = datahub

    val productHolderProbe = TestProbe("productHolder")
    val product = ProductEntity(3)
    val productData = ProductOps.zero
    val productFacade = ActorFacade(product, productHolderProbe.ref)

    // cache of product data
    val warehouseHolderProbe = TestProbe("warehouseHolder")
    val warehouseData = ALOData(product.id)(ClockInt(System.currentTimeMillis, 0L))
    val warehouseFacade = ActorFacade(WarehouseEntity("Warehouse1"), warehouseHolderProbe.ref)

    // Register product
    hub.register(productFacade)(
      productData.clock.asInstanceOf[productFacade.entity.ops.D#C],
      Map.empty,
      Set.empty
    )
    expectMsgType[Unit]

    // Register warehouse which depends on product, get updates from it
    hub.register(warehouseFacade)(
      warehouseData.clock.asInstanceOf[warehouseFacade.entity.ops.D#C],
      Map(product → productData.clock),
      Set.empty
    )
    expectMsgType[Unit]

    // Product entity data is updated
    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
    val productDataUpdateRes = hub.dataUpdated(product, Set.empty)(newProductData)

    val res = warehouseHolderProbe.expectMsgType[RelatedDataUpdated]
    res.data.clock shouldEqual newProductData.clock
    warehouseHolderProbe.lastSender ! ()

    productDataUpdateRes.futureValue
  }

  it should "subscribe entity to new related entities" in {
    val hub = datahub

    val productHolderProbe = TestProbe("productHolder")
    val product = ProductEntity(4)
    val productFacade = ActorFacade(product, productHolderProbe.ref)

    // cache of product data
    val warehouseHolderProbe = TestProbe("warehouseHolder")
    val warehouse = WarehouseEntity("1")
    val warehouseFacade = ActorFacade(warehouse, warehouseHolderProbe.ref)

    // Register product
    hub.register(productFacade)(
      ProductOps.zero.clock.asInstanceOf[productFacade.entity.ops.D#C],
      Map.empty, Set.empty
    ).futureValue

    // Register warehouse
    hub.register(warehouseFacade)(
      WarehouseOps.zero.clock.asInstanceOf[warehouseFacade.entity.ops.D#C],
      Map.empty, Set.empty
    ).futureValue

    // Send update with new related entity
    val newWarehouseData = warehouse.ops.zero.updated(product.productId, System.currentTimeMillis)
    hub.dataUpdated(warehouse, Set.empty)(newWarehouseData).futureValue

    // Product entity data is updated
    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
    hub.dataUpdated(product, Set.empty)(newProductData).futureValue

    warehouseHolderProbe.expectMsgType[RelatedDataUpdated].data.clock shouldEqual newProductData.clock
    warehouseHolderProbe.lastSender ! ()
  }
}