package ru.oseval.datahub.domain

import ru.oseval.datahub.Entity
import ru.oseval.datahub.data.InferredOps.InferredOps
import ru.oseval.datahub.data._
import ru.oseval.datahub.domain.ProductTestData.ProductEntity

object WarehouseTestData {
  type WarehouseId = String

  case class WarehouseData(products: SetData[Int, Long]) extends Data {
    override val clock: Long = products.lastClockOpt getOrElse 0L
    override type C = Long
  }

  object WarehouseOps extends ALODataOps[InferredOps[WarehouseData]] {
    override protected val ops: InferredOps[WarehouseData] = InferredOps(
      WarehouseData(SetDataOps.zero[Int, Long]),
      "warehouse",
      _.products.elements.map(ProductEntity(_): Entity).toSet -> Set.empty[Entity]
    )

    override def diffFromClock(a: ALOData[WarehouseData], from: Long): ALOData[WarehouseData] =
      if (a.clock >= from)
        ALOData(WarehouseData(products = SetDataOps.diffFromClock(a.data.products, from)))(from)
      else WarehouseOps.zero
  }

  case class WarehouseEntity(warehouseId: WarehouseId) extends Entity {
    lazy val id: String = WarehouseOps.kind + "_" + warehouseId
    val ops = WarehouseOps
  }
}
