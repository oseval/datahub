package ru.oseval.datahub

import ru.oseval.datahub.ProductTestData.ProductEntity
import ru.oseval.datahub.data._

object WarehouseTestData {
  type WarehouseId = String

  case class WarehouseData(products: Set[Int], clock: Long = 0L) extends Data {
    override type C = Long
  }

  object WarehouseOpsSimple extends DataOps {
    override val kind: String = "warehouse"
    override type D = WarehouseData
    override val ordering = Ordering.Long
    override val zero = WarehouseData(Set.empty)

    override def nextClock(current: Long): Long = System.currentTimeMillis max (current + 1)

    override def getRelations(data: D): (Set[Entity], Set[Entity]) = data.products.map(ProductEntity(_): Entity) -> Set.empty[Entity]
  }

  object WarehouseOps extends ALODataOps[WarehouseOpsSimple.type] {
    override protected val ops: WarehouseOpsSimple.type = WarehouseOpsSimple
    override def nextClock(current: Long): Long = ops.nextClock(current)
  }

  case class WarehouseEntity(warehouseId: WarehouseId) extends Entity {
    lazy val id: String = WarehouseOps.kind + "_" + warehouseId
    val ops = WarehouseOps
  }
}
