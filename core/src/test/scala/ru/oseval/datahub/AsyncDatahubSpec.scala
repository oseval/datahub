package ru.oseval.datahub

import java.util.concurrent.{Callable, ScheduledThreadPoolExecutor, ThreadPoolExecutor, TimeUnit}

import org.scalatest
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.mockito.Mockito._

import scala.concurrent.duration._
import ProductTestData._
import WarehouseTestData._
import org.scalatest.mockito.MockitoSugar
import org.slf4j.LoggerFactory
import ru.oseval.datahub
import ru.oseval.datahub.data.{ALOData, ClockInt}

import scala.concurrent.Future

class AsyncDatahubSpec extends FlatSpecLike
  with BeforeAndAfterAll
  with MockitoSugar
  with ScalaFutures
  with scalatest.Matchers
  with Eventually
  with CommonTestMethods {

  behavior of "Datahub"

  it should "register data entities" in {
    val facade = mock[EntityFacade]
    val datahub = createDatahub

    when(facade.entity).thenReturn(ProductEntity(1))

    datahub.register(facade)
  }

//  it should "subscribe on related data entities" in {
//    val datahub = createDatahub
//
//    val product = ProductEntity(2)
//    val productFacade = mock[EntityFacade { val entity: product.type }]
//    when(productFacade.entity).thenReturn(product)
//    val productData = ProductOps.zero
//
//    val subscriber = mock[Subscriber]
//
//    datahub.register(productFacade)
//
//    // Product entity data is updated
//    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
//
//    datahub.dataUpdated(product)(newProductData)
//
//    // Register warehouse which depends on product, get updates from it
//    when(productFacade.getUpdatesFrom(productData.clock)).thenReturn(Future.successful(newProductData))
////    when(warehouseFacade.onUpdate(product.id, newProductData)).thenReturn(Future.unit)
//
////    datahub.register(warehouseFacade)(warehouseData.clock, Map(product -> ProductOps.zero.clock), Set.empty).futureValue
//
//    eventually {
//      verify(productFacade).getUpdatesFrom(productData.clock)
//      verify(subscriber).onUpdate(product.id, newProductData)
//    }
//  }
//
//  it should "receive updates from related entities" in {
//    val datahub = createDatahub
//
//    val product = ProductEntity(3)
//    val productFacade = mock[EntityFacade { val entity: product.type }]
//    when(productFacade.entity).thenReturn(product)
//    val productData = ProductOps.zero
//
//    // cache of product data
//    val warehouse = WarehouseEntity("Warehouse1")
//    val warehouseData = ALOData(productData)(ClockInt(System.currentTimeMillis, 0L))
//    val warehouseFacade = mock[EntityFacade { val entity: warehouse.type }]
//    when(warehouseFacade.entity).thenReturn(warehouse)
//
//    // Register product
//    datahub.register(productFacade)(productData.clock, Map.empty, Set.empty).futureValue
//
//    // Register warehouse which depends on product, get updates from it
//    when(productFacade.requestForApprove(warehouse)).thenReturn(Future.successful(true))
//
//    datahub.register(warehouseFacade)(warehouseData.clock, Map(product -> productData.clock), Set.empty).futureValue
//
//    verify(productFacade).requestForApprove(warehouse)
//
//    // Product entity data is updated
//    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
//    datahub.dataUpdated(product, Set.empty)(newProductData).futureValue
//
//    verify(warehouseFacade).onUpdate(product.id, newProductData)
//  }
//
//  // TODO: move it to LocalStorageSpec
//  it should "subscribe entity on new related entities" in {
//    val datahub = createDatahub
//
//    val product = ProductEntity(1)
//    val productFacade = mock[EntityFacade]
//    when(productFacade.entity).thenReturn(product)
//    val productZero: productFacade.entity.ops.D = productFacade.entity.ops.zero
//    val productZeroClock: productFacade.entity.ops.D#C = productFacade.entity.ops.zero.clock
//
//    // cache of product data
//    val warehouse = WarehouseEntity("1")
//    val warehouseFacade = mock[EntityFacade]
//    when(warehouseFacade.entity).thenReturn(warehouse)
//
//    // Register product
//    datahub.register(productFacade)(productZeroClock, Map.empty, Set.empty).futureValue
//
//    // Register warehouse
//    datahub.register(warehouseFacade)(warehouseFacade.entity.ops.zero.clock, Map.empty, Set.empty).futureValue
//
//    // Send update with new related entity
//    when(productFacade.requestForApprove(warehouse)).thenReturn(Future.successful(true))
//    when(productFacade.getUpdatesFrom(productZeroClock)).thenReturn(Future.successful(productZero))
//
////    val newWarehouseData = warehouse.ops.zero.updated(product.productId, System.currentTimeMillis)
////    datahub.dataUpdated(warehouse, Set.empty)(newWarehouseData).futureValue
//
//    datahub.subscribe(product, warehouse, None)
//
//    verify(productFacade).requestForApprove(warehouse)
//
//    // Product entity data is updated
//    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
//    datahub.dataUpdated(product, Set.empty)(newProductData).futureValue
//
//    verify(warehouseFacade).onUpdate(product.id, newProductData)
//  }
//
//  it should "request entity and send clocks after sync request" in {
//    val datahub = createDatahub
//
//    val product = ProductEntity(5)
//    val productFacade = mock[EntityFacade]
//    when(productFacade.entity).thenReturn(product)
//
//    // cache of product data
//    val warehouse = WarehouseEntity("5")
//    val warehouseFacade = mock[EntityFacade { val entity: warehouse.type }]
//    val warehouseData = ALOData(product.id)(ClockInt(System.currentTimeMillis, 0L))
//    when(warehouseFacade.entity).thenReturn(warehouse)
//
//    // Register product
//    datahub.register(productFacade)(productFacade.entity.ops.zero.clock, Map.empty, Set.empty).futureValue
//
//    // Register warehouse
//    when(productFacade.requestForApprove(warehouse)).thenReturn(Future.successful(true))
//
//    datahub.register(warehouseFacade)(
//      warehouseData.clock, Map(product -> productFacade.entity.ops.zero.clock), Set.empty
//    ).futureValue
//
//    verify(productFacade).requestForApprove(warehouse)
//
//    // Product entity data is updated
//    val newProductData = ProductData("TV", 1, System.currentTimeMillis)
//    datahub.dataUpdated(product, Set.empty)(newProductData).futureValue
//
//    verify(warehouseFacade).onUpdate(product.id, newProductData)
//
//    datahub.syncRelationClocks(warehouse, Map(product -> product.ops.zero.clock))
//
//    verify(warehouseFacade).onUpdate(product.id, newProductData)
//  }
}