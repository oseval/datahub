package com.github.oseval.datahub

import org.scalatest.{FlatSpecLike, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.slf4j.LoggerFactory
import org.mockito.Mockito._
import com.github.oseval.datahub.data.InferredOps.InferredOps
import com.github.oseval.datahub.domain.ProductTestData.{ProductData, ProductEntity, ProductOps}
import com.github.oseval.datahub.domain.WarehouseTestData.{WarehouseData, WarehouseEntity, WarehouseOps}
import com.github.oseval.datahub.data._

import scala.ref.WeakReference

class LocalStorageSpec extends FlatSpecLike
  with MockitoSugar
  with ScalaFutures
  with Matchers {

  val product1 = ProductEntity(1)
  val product2 = ProductEntity(2)
  val warehouse1 = WarehouseEntity("1")
  val warehouse2 = WarehouseEntity("2")

  val time = System.currentTimeMillis
  val productData = ProductData("p1", 1, 4L)
  val productDataTotal = ProductOps.combine(ProductOps.zero, productData)
  val warehouseData1 = {
    val productSet = SetData.one(product1.productId, time + 3)
    ALOData(WarehouseData(productSet))(0)
  }
  val warehouseData2 = warehouseData1.updated(WarehouseData(SetData.one(product2.productId, time + 4)))
  val warehouseDataTotal = WarehouseOps.combine(warehouseData1, warehouseData2)

  val log = LoggerFactory.getLogger(getClass)

  class SpiedDatahub extends Datahub {
    override def register(facade: EntityFacade): Unit = ()
    override def subscribe(entity: Entity,
                           subscriber: Subscriber,
                           lastKnownDataClock: Any): Boolean = true
    override def unsubscribe(entity: Entity, subscriber: Subscriber): Unit = ()
    override def dataUpdated(entity: Entity)(data: entity.ops.D): Unit = ()
    override def syncRelationClocks(entity: Subscriber,
                                    relationClocks: Map[Entity, Any]): Unit = ()
  }

  def makeStorage(knownData: Map[Entity, Data] = Map.empty): (LocalDataStorage, Datahub) = {
    val datahub = new SpiedDatahub
    val spiedhub = spy[SpiedDatahub](datahub)
    new LocalDataStorage(WeakReference(spiedhub), _ => null, LoggerFactory.getLogger(getClass), (_, _) => (), knownData) -> spiedhub
  }

  val (storage, listener) = makeStorage()

  behavior of "LocalStorage"

  it should "register entity with right relation clocks" in {
    storage.addEntity(warehouse1)(warehouseData1)

    verify(listener).register(
      null.asInstanceOf[LocalEntityFacade { val entity: warehouse1.type }]
    )
  }

  case class WarehouseDependentData(warehouseE: Option[WarehouseEntity], clock: Long) extends Data {
    override type C = Long
  }

  object WarehouseDependentDataOps extends InferredOps[WarehouseDependentData](
    WarehouseDependentData(None, 0L), "wdepdata", InferredOps.timeClockBehavior
  ) with OpsWithRelations[WarehouseDependentData] {
    override def getRelations(data: WarehouseDependentData): (Set[Entity], Set[Entity]) = (data.warehouseE.toSet, Set.empty)
  }

  case class WarehouseDependentEntity(xid: Int) extends Entity {
    val ops = WarehouseDependentDataOps
    override val id: String = ops.kind + "_" + xid
  }

  it should "sync relation when it is not solid" in {
    val (s2, dh) = makeStorage()
    s2.addEntity(WarehouseDependentEntity(5))(WarehouseDependentData(Some(warehouse1), 1L))
    s2.onUpdate(warehouse1)(warehouseData1.copy[WarehouseData](clock = 2L, previousClock = 1L)) // product is acid - always is solid

    s2.checkDataIntegrity shouldBe false
    verify(dh).syncRelationClocks(s2, Map(warehouse1 -> 0L))
  }

  it should "notify when local entity updated" in {
    storage.addEntity(product1)(productData)
    storage.combineEntity(warehouse1)(_ => warehouseData2)

    verify(listener).dataUpdated(warehouse1)(warehouseData2)

    storage.get(warehouse1) shouldBe Some(warehouseDataTotal)
    storage.get(product1) shouldBe Some(productDataTotal)
    storage.get(product2) shouldBe Some(ProductOps.zero)
  }

  it should "subscribe entity on new related entities" in {
    storage.addEntity(warehouse2)(warehouseData1)

    reset(listener)
    storage.onUpdate(product2)(productData)
    verifyZeroInteractions(listener)

    storage.combineEntity(warehouse2)(_ => warehouseDataTotal)
    verify(listener).subscribe(product2, storage, productDataTotal.clock)
  }
}
