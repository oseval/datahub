package ru.oseval.datahub

import org.mockito.Mockito
import org.scalatest.{FlatSpecLike, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.slf4j.LoggerFactory
import org.mockito.Mockito._
import ru.oseval.datahub.Datahub.{DataUpdated, Register}
import ru.oseval.datahub.ProductTestData.{ProductData, ProductEntity}
import ru.oseval.datahub.WarehouseTestData.{WarehouseData, WarehouseEntity}
import ru.oseval.datahub.data.Data

import scala.concurrent.Future

class LocalStorageSpec extends FlatSpecLike
  with MockitoSugar
  with ScalaFutures
  with Matchers {

  val product1 = ProductEntity(1)
  val product2 = ProductEntity(2)
  val product3 = ProductEntity(3)
  val warehouse1 = WarehouseEntity("1")
  val warehouse2 = WarehouseEntity("2")

  val time = System.currentTimeMillis
  val product1Data = ProductData("Product1", 4, time)
  val product2Data = ProductData("Product2", 7, time + 1)
  val product3Data = ProductData("Product3", 35, time + 2)
  val warehouseData1 = WarehouseData(Map((time + 3) -> product1.id))
  val warehouseData2 = WarehouseData(Map((time + 4) -> product2.id))
  val warehouseDataTotal = WarehouseData(Map((time + 3) -> product1.id, (time + 4) -> product2.id))

  val log = LoggerFactory.getLogger(getClass)

  class MessageListener {
    def notify(msg: Datahub.DatahubMessage): Future[Unit] = Future.unit
  }

  def makeStorage(knownData: Map[Entity, Data] = Map.empty): (LocalDataStorage, MessageListener) = {
    val listener = new MessageListener
    val spiedListener = Mockito.spy[MessageListener](listener)
    new LocalDataStorage(
      LoggerFactory.getLogger(getClass),
      _ => null,
      msg => spiedListener.notify(msg),
      knownData
    ) -> spiedListener
  }

  val (storage, listener) = makeStorage()

  behavior of "LocalStorage"

  it should "register relation and combine it's data" in {
    storage.addRelation(product1)
    storage.combine(product1.id, product1Data)
    storage.get(product1) shouldBe Some(product1Data)
  }

  it should "register entity with right relation clocks" in {
    storage.addEntity(warehouse1)(warehouseData1).futureValue

    verify(listener).notify(Register(
      null.asInstanceOf[EntityFacade { val entity: warehouse1.type }],
      Map(product1.id -> product1Data.clock)
    )(warehouseData1.clock))
  }

  it should "notify when local entity updated" in {
    storage.combine(warehouse1.id, warehouseData2).futureValue

    verify(listener).notify(DataUpdated(warehouse1.id, warehouseData2))

    storage.get(warehouse1) shouldBe Some(warehouseDataTotal)
  }
}
