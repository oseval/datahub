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

import scala.concurrent.Future

class DatahubSpec extends FlatSpecLike
  with BeforeAndAfterAll
  with MockitoSugar
  with ScalaFutures
  with scalatest.Matchers
  with Eventually {

  private val ec = scala.concurrent.ExecutionContext.global
  private implicit val timeout = 3.seconds

  def storage = new MemoryStorage

  behavior of "Datahub"

  it should "register data entities" in {
    val facade = mock[EntityFacade]
    val datahub = new Datahub(storage, ec) {}

    when(facade.entity).thenReturn(ProductEntity(1))

    datahub.receive(Register(facade, Map.empty)(facade.entity.ops.zero.clock)).futureValue
  }

  it should "subscribe on related data entities" in {
    val datahub = new Datahub(storage, ec) {}

    val product = ProductEntity(2)
    val productFacade = mock[EntityFacade { val entity: product.type }]
    when(productFacade.entity).thenReturn(product)
    val productData = ProductOps.zero

    // cache of product data
    val warehouse = WarehouseEntity("Warehouse1")
    val warehouseFacade = mock[EntityFacade { val entity: warehouse.type }]
    when(warehouseFacade.entity).thenReturn(warehouse)
    val knownClocks = System.currentTimeMillis
    val warehouseData = WarehouseData(Map(knownClocks -> product.id))

    // Register product
    datahub.receive(Register(productFacade, Map.empty)(productData.clock)).futureValue

    // Product entity data is updated
    val newProductData = ProductData("TV", 1, knownClocks + 1L)

    datahub.receive(DataUpdated(product.id, newProductData)).futureValue

    // Register warehouse which depends on product, get updates from it
    when(productFacade.getUpdatesFrom(productData.clock)).thenReturn(Future.successful(newProductData))
    when(warehouseFacade.onUpdate(product.id, newProductData)).thenReturn(Future.unit)

    datahub.receive(
      Register(warehouseFacade, Map(product.id -> ProductOps.zero.clock))(warehouseData.clock)
    ).futureValue

    verify(productFacade).getUpdatesFrom(productData.clock)
    verify(warehouseFacade).onUpdate(product.id, newProductData)
  }

  it should "receive updates from related entities" in {
    val datahub = new Datahub(storage, ec) {}

    val product = ProductEntity(3)
    val productFacade = mock[EntityFacade { val entity: product.type }]
    when(productFacade.entity).thenReturn(product)
    val productData = ProductOps.zero

    // cache of product data
    val warehouse = WarehouseEntity("Warehouse1")
    val warehouseData = WarehouseData(Map(System.currentTimeMillis -> product.id))
    val warehouseFacade = mock[EntityFacade { val entity: warehouse.type }]
    when(warehouseFacade.entity).thenReturn(warehouse)

    // Register product
    datahub.receive(Register(productFacade, Map.empty)(productData.clock)).futureValue

    // Register warehouse which depends on product, get updates from it
    datahub.receive(Register(warehouseFacade, Map(product.id → productData.clock))(warehouseData.clock)).futureValue

    // Product entity data is updated
    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
    datahub.receive(DataUpdated(product.id, newProductData)).futureValue

    verify(warehouseFacade).onUpdate(product.id, newProductData)
  }

  it should "subscribe entity to new related entities" in {
    val datahub = new Datahub(storage, ec) {}

    val product = ProductEntity(1)
    val productFacade = mock[EntityFacade]
    when(productFacade.entity).thenReturn(product)

    // cache of product data
    val warehouse = WarehouseEntity("1")
    val warehouseFacade = mock[EntityFacade]
    when(warehouseFacade.entity).thenReturn(warehouse)

    // Register product
    datahub.receive(Register(productFacade, Map.empty)(productFacade.entity.ops.zero.clock)).futureValue

    // Register warehouse
    datahub.receive(Register(warehouseFacade, Map.empty)(warehouseFacade.entity.ops.zero.clock)).futureValue

    // Send update with new related entity
    val newWarehouseData = WarehouseData(Map(System.currentTimeMillis → product.id))
    datahub.receive(DataUpdated(warehouse.id, newWarehouseData)).futureValue

    // Product entity data is updated
    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
    datahub.receive(DataUpdated(product.id, newProductData)).futureValue

    verify(warehouseFacade).onUpdate(product.id, newProductData)
  }
}