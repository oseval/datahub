package ru.oseval.datahub.domain

import ru.oseval.datahub.Entity
import ru.oseval.datahub.data.InferredOps.InferredOps
import ru.oseval.datahub.data.{ALODataOps, Data, InferredOps}
import ru.oseval.datahub.domain.ProductTestData.ProductEntity

object WarehouseTestData {
  type WarehouseId = String

  case class WarehouseData(products: Set[Int], clock: Long = 0L) extends Data {
    override type C = Long
  }

  object WarehouseOps extends ALODataOps[InferredOps[WarehouseData]] {
    override protected val ops: InferredOps[WarehouseData] = InferredOps(
      WarehouseData(Set.empty),
      "warehouse",
      _.products.map(ProductEntity(_): Entity) -> Set.empty[Entity]
    )
  }

  case class WarehouseEntity(warehouseId: WarehouseId) extends Entity {
    lazy val id: String = WarehouseOps.kind + "_" + warehouseId
    val ops = WarehouseOps
  }
}
