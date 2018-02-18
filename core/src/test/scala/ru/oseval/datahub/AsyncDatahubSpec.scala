package ru.oseval.datahub

import org.scalatest
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.mockito.Mockito._
import ru.oseval.datahub.Datahub._

import scala.concurrent.duration._
import ProductTestData._
import WarehouseTestData._
import org.scalatest.mockito.MockitoSugar
import ru.oseval.datahub.data.{ALOData, ClockInt}

import scala.concurrent.Future

class AsyncDatahubSpec extends FlatSpecLike
  with BeforeAndAfterAll
  with MockitoSugar
  with ScalaFutures
  with scalatest.Matchers
  with Eventually {

  private val ec = scala.concurrent.ExecutionContext.global
  private implicit val timeout = 3.seconds

  def storage = new MemoryStorage
  def createDatahub = new AsyncDatahub(storage)(ec)

  behavior of "Datahub"

  it should "register data entities" in {
    val facade = mock[EntityFacade]
    val datahub = createDatahub

    when(facade.entity).thenReturn(ProductEntity(1))

    datahub.register(facade)(facade.entity.ops.zero.clock, Map.empty).futureValue
  }

  it should "subscribe on related data entities" in {
    val datahub = createDatahub

    val product = ProductEntity(2)
    val productFacade = mock[EntityFacade { val entity: product.type }]
    when(productFacade.entity).thenReturn(product)
    val productData = ProductOps.zero

    // cache of product data
    val warehouse = WarehouseEntity("Warehouse1")
    val warehouseFacade = mock[EntityFacade { val entity: warehouse.type }]
    when(warehouseFacade.entity).thenReturn(warehouse)
    val knownClocks = System.currentTimeMillis
    val warehouseData = ALOData(product.id)(ClockInt(knownClocks, 0L))

    // Register product
    datahub.register(productFacade)(productData.clock, Map.empty).futureValue

    // Product entity data is updated
    val newProductData = ProductData("TV", 1, System.currentTimeMillis)

    datahub.dataUpdated(product.id, newProductData).futureValue

    // Register warehouse which depends on product, get updates from it
    when(productFacade.getUpdatesFrom(productData.clock)).thenReturn(Future.successful(newProductData))
    when(productFacade.requestForApprove(warehouse)).thenReturn(Future.successful(true))
    when(warehouseFacade.onUpdate(product.id, newProductData)).thenReturn(Future.unit)

    datahub.register(warehouseFacade)(warehouseData.clock, Map(product.id -> ProductOps.zero.clock)).futureValue

    eventually {
      verify(productFacade).getUpdatesFrom(productData.clock)
      verify(productFacade).requestForApprove(warehouse)
      verify(warehouseFacade).onUpdate(product.id, newProductData)
    }
  }

  it should "receive updates from related entities" in {
    val datahub = createDatahub

    val product = ProductEntity(3)
    val productFacade = mock[EntityFacade { val entity: product.type }]
    when(productFacade.entity).thenReturn(product)
    val productData = ProductOps.zero

    // cache of product data
    val warehouse = WarehouseEntity("Warehouse1")
    val warehouseData = ALOData(product.id)(ClockInt(System.currentTimeMillis, 0L))
    val warehouseFacade = mock[EntityFacade { val entity: warehouse.type }]
    when(warehouseFacade.entity).thenReturn(warehouse)

    // Register product
    datahub.register(productFacade)(productData.clock, Map.empty).futureValue

    // Register warehouse which depends on product, get updates from it
    when(productFacade.requestForApprove(warehouse)).thenReturn(Future.successful(true))

    datahub.register(warehouseFacade)(warehouseData.clock, Map(product.id → productData.clock)).futureValue

    verify(productFacade).requestForApprove(warehouse)

    // Product entity data is updated
    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
    datahub.dataUpdated(product.id, newProductData).futureValue

    verify(warehouseFacade).onUpdate(product.id, newProductData)
  }

  it should "subscribe entity to new related entities" in {
    val datahub = createDatahub

    val product = ProductEntity(1)
    val productFacade = mock[EntityFacade]
    when(productFacade.entity).thenReturn(product)

    // cache of product data
    val warehouse = WarehouseEntity("1")
    val warehouseFacade = mock[EntityFacade]
    when(warehouseFacade.entity).thenReturn(warehouse)

    // Register product
    datahub.register(productFacade)(productFacade.entity.ops.zero.clock, Map.empty).futureValue

    // Register warehouse
    datahub.register(warehouseFacade)(warehouseFacade.entity.ops.zero.clock, Map.empty).futureValue

    // Send update with new related entity
    when(productFacade.requestForApprove(warehouse)).thenReturn(Future.successful(true))

    val newWarehouseData = ALOData(product.id)(ClockInt(System.currentTimeMillis, 0L))
    datahub.dataUpdated(warehouse.id, newWarehouseData).futureValue

    verify(productFacade).requestForApprove(warehouse)

    // Product entity data is updated
    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
    datahub.dataUpdated(product.id, newProductData).futureValue

    verify(warehouseFacade).onUpdate(product.id, newProductData)
  }

  it should "request entity and send clocks after sync request" in {
    val datahub = createDatahub

    val product = ProductEntity(5)
    val productFacade = mock[EntityFacade]
    when(productFacade.entity).thenReturn(product)

    // cache of product data
    val warehouse = WarehouseEntity("5")
    val warehouseFacade = mock[EntityFacade { val entity: warehouse.type }]
    val warehouseData = ALOData(product.id)(ClockInt(System.currentTimeMillis, 0L))
    when(warehouseFacade.entity).thenReturn(warehouse)

    // Register product
    datahub.register(productFacade)(productFacade.entity.ops.zero.clock, Map.empty).futureValue

    // Register warehouse
    when(productFacade.requestForApprove(warehouse)).thenReturn(Future.successful(true))

    datahub.register(warehouseFacade)(
      warehouseData.clock, Map(product.id -> productFacade.entity.ops.zero.clock)
    ).futureValue

    verify(productFacade).requestForApprove(warehouse)

    // Product entity data is updated
    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
    datahub.dataUpdated(product.id, newProductData).futureValue

    verify(warehouseFacade).onUpdate(product.id, newProductData)

    datahub.syncRelationClocks(warehouse.id, Map(product.id -> product.ops.zero.clock))

    verify(warehouseFacade).onUpdate(product.id, newProductData)
  }
}