package ru.oseval.datahub

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.scalatest
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import ru.oseval.datahub.AkkaDatahub.{DataUpdated, Register}
import ActorFacadeMessages._
import org.mockito.{Matchers, Mockito}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import ru.oseval.datahub.data.ALOData

import scala.concurrent.Future

class DataHolderSpec extends TestKit(ActorSystem("holderTest"))
  with ImplicitSender
  with FlatSpecLike
  with MockitoSugar
  with BeforeAndAfterAll
  with ScalaFutures
  with scalatest.Matchers
  with Eventually {

  import ProductTestData._
  import ActorProductTestData._
  import WarehouseTestData._
  import ActorWarehouseTestData._
  import org.mockito.Matchers._

  private def eqq[T] = Matchers.eq[T](_)

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  private val datahub = spy(new AkkaDatahub(new MemoryStorage)(system, system.dispatcher))

  behavior of "Data holder"

  it should "response on get difference from id" in {
    val product = ProductEntity(1)
    val productHolder = system.actorOf(productProps(product.productId, datahub))

    productHolder ! Ping
    expectMsg(Pong)
    verify(datahub).register(any())(any(), any(), any())

    productHolder ! GetDifferenceFrom(product.id, product.ops.zero.clock)
    expectMsgType[ProductData] shouldEqual product.ops.zero

    val newProductData = ProductData("Product name", 1, System.currentTimeMillis)
    productHolder ! UpdateData(newProductData)
    expectMsgType[Unit]
    verify(datahub).dataUpdated(product, Set.empty)(newProductData)
    reset(datahub)

    productHolder ! GetDifferenceFrom(product.id, product.ops.zero.clock)
    expectMsgType[ProductData] shouldEqual newProductData
  }

  it should "send data update to notifier" in {
    val product = ProductEntity(2)
    val productHolder = system.actorOf(productProps(product.productId, datahub))

    productHolder ! Ping
    expectMsg(Pong)
    verify(datahub).register(any())(any(), eqq(Map.empty), eqq(Set.empty))

    val productData = ProductData("Product name", 1, System.currentTimeMillis)
    productHolder ! UpdateData(productData)
    expectMsgType[Unit]
    verify(datahub).dataUpdated(product, Set.empty)(productData)
    reset(datahub)
  }

  it should "update related data" in {
    val product = ProductEntity(3)
    val warehouse = WarehouseEntity("1")
    val warehouseHolder = system.actorOf(warehouseProps(warehouse.warehouseId, datahub))

    verify(datahub, Mockito.timeout(3000)).register(any())(any(), eqq(Map.empty), eqq(Set.empty))

    warehouseHolder ! GetDifferenceFrom(warehouse.id, WarehouseOps.zero.clock)
    expectMsgType[ALOData[String]] shouldEqual WarehouseOps.zero.copy(previousClock = WarehouseOps.zero.clock)

    warehouseHolder ! AddProduct(product.productId)
    expectMsgType[Unit]
    verify(datahub).dataUpdated(eqq(warehouse), eqq(Set.empty))(any())

    warehouseHolder ! GetDifferenceFrom(warehouse.id, WarehouseOps.zero.clock)
    expectMsgType[ALOData[String]].elements should contain(product.productId)

    val productData = ProductData("Product name", 1, System.currentTimeMillis)

    warehouseHolder ! RelatedDataUpdated(warehouse.id, product.id, productData)
    expectMsgType[Unit]
  }
}
