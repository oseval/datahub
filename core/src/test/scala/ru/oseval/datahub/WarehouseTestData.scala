package ru.oseval.datahub

import ru.oseval.datahub.ProductTestData._

object WarehouseTestData {
  type WarehouseId = String
  type WarehouseClock = String

  case class WarehouseData(products: Map[WarehouseClock, ProductId]) extends Data { self ⇒
    override val clock: WarehouseClock = if (products.isEmpty) "0" else products.keySet.maxBy(_.toLong)
  }

  object WarehouseOps extends DataOps[WarehouseData] {
    override val ordering: Ordering[WarehouseClock] = Data.timestampOrdering
    override val zero: WarehouseData = WarehouseData(Map.empty)

    override def combine(a: WarehouseData, b: WarehouseData): WarehouseData =
      WarehouseData(a.products ++ b.products)

    override def diffFromClock(data: WarehouseData, from: WarehouseClock): WarehouseData =
      WarehouseData(products = data.products.filterKeys(ordering.gt(_, from)))

    override def getRelations(data: WarehouseData): Set[ProductId] =
      data.products.values.toSet

    override def makeId(ownId: Any): String = "warehouse_" + ownId
  }

  case class WarehouseEntity(ownId: String) extends Entity {
    override type D = WarehouseData
    val ops = WarehouseOps
  }
}
