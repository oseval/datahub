package ru.oseval.datahub

import org.mockito.Mockito
import org.scalatest.{FlatSpecLike, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.slf4j.LoggerFactory
import org.mockito.Mockito._
import ru.oseval.datahub.ProductTestData.{ProductData, ProductEntity}
import ru.oseval.datahub.WarehouseTestData.{WarehouseEntity, WarehouseOps}
import ru.oseval.datahub.data.{CumulativeData, ClockInt, Data}

class LocalStorageSpec extends FlatSpecLike
  with MockitoSugar
  with ScalaFutures
  with Matchers {

  val product1 = ProductEntity(1)
  val product2 = ProductEntity(2)
  val warehouse1 = WarehouseEntity("1")
  val warehouse2 = WarehouseEntity("2")

  val time = System.currentTimeMillis
  val product1Data = ProductData("Product1", 4, time)
  val warehouseData1 = CumulativeData(product1.productId)(ClockInt(time + 3, 0L))
  val warehouseData2 = warehouseData1.updated(product2.productId, time + 4)
  val warehouseDataTotal = WarehouseOps.combine(warehouseData1, warehouseData2)

  // need at least once data here because we tests not solid data
  val time2 = System.currentTimeMillis
  val warehouse2Data1 = CumulativeData(warehouse2.id)(ClockInt(time2, 0L))
  val warehouse2Data2 = warehouse2Data1.updated(product1.id, time2 + 1)
  val warehouse2Data3 = warehouse2Data2.updated(product1.id, time2 + 2)

  val log = LoggerFactory.getLogger(getClass)

  type Id[T] = T
  class SpiedDatahub extends Datahub[Id] {
    override def register(facade: EntityFacade)
                         (lastClock: facade.entity.ops.D#C,
                          relationClocks: Map[Entity, Any],
                          forcedSubscribers: Set[EntityFacade]): Id[Unit] = ()
    override def subscribe(entity: Entity,
                           subscriber: Entity,
                           lastKnownDataClockOpt: Option[Any]): Unit = ()
    override def unsubscribe(entity: Entity, subscriber: Entity): Unit = ()
    override def dataUpdated(entity: Entity, forcedSubscribers: Set[EntityFacade])(data: entity.ops.D): Id[Unit] = ()
    override def syncRelationClocks(entity: Entity,
                                    relationClocks: Map[Entity, Any]): Id[Unit] = ()
  }

  def makeStorage(knownData: Map[Entity, Data] = Map.empty): (LocalDataStorage[Id], Datahub[Id]) = {
    val datahub = new SpiedDatahub
    val spiedhub = Mockito.spy[SpiedDatahub](datahub)
    new LocalDataStorage(LoggerFactory.getLogger(getClass), _ => null, spiedhub, knownData) -> spiedhub
  }

  val (storage, listener) = makeStorage()

  behavior of "LocalStorage"

  it should "register relation and combine it's data" in {
    storage.addRelation(product1)
    storage.combineRelation(product1.id, product1Data)
    storage.get(product1) shouldBe Some(product1Data)
  }

  it should "register entity with right relation clocks" in {
    storage.addEntity(warehouse1)(warehouseData1)

    verify(listener).register(
      null.asInstanceOf[EntityFacade { val entity: warehouse1.type }]
    )(warehouseData1.clock, Map(product1 -> product1Data.clock), Set.empty)
  }

  it should "sync relation when it is not solid" in {
    storage.addRelation(warehouse2)
    storage.combineRelation(warehouse2.id, warehouse2Data1)
    storage.combineRelation(warehouse2.id, warehouse2Data3)

    storage.checkDataIntegrity shouldBe false
    verify(listener).syncRelationClocks(warehouse1, Map(warehouse2 -> warehouse2Data1.clock))
  }

  it should "notify when local entity updated" in {
    storage.combineEntity(warehouse1)(_ => warehouseData2)

    verify(listener).dataUpdated(warehouse1, Set.empty)(warehouseData2)

    storage.get(warehouse1) shouldBe Some(warehouseDataTotal)
    storage.get[ProductData](product1.id) shouldBe Some(product1Data)
    storage.get[ProductData](product2.id) shouldBe None
  }
}
