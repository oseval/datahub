package com.github.oseval.datahub.domain

import com.github.oseval.datahub.Entity
import com.github.oseval.datahub.data.InferredOps.InferredOps
import com.github.oseval.datahub.data._
import com.github.oseval.datahub.domain.ProductTestData.ProductEntity

object WarehouseTestData {
  type WarehouseId = String

  case class WarehouseData(products: SetData[Int, Long]) extends Data {
    override val clock: Long = products.lastClockOpt getOrElse 0L
    override type C = Long
  }

  object WarehouseOps extends ALODataOps[InferredOps[WarehouseData]] with OpsWithRelations[ALOData[WarehouseData]] {
    override def getRelations(data: ALOData[WarehouseData]): (Set[Entity], Set[Entity]) =
      data.data.products.elements.map(ProductEntity(_): Entity).toSet -> Set.empty[Entity]

    override protected val ops: InferredOps[WarehouseData] = InferredOps(
      WarehouseData(SetDataOps.zero[Int, Long]),
      "warehouse"
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
