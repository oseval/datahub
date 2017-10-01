package ru.oseval.datahub

import ru.oseval.datahub.data.{Data, DataOps}

object WarehouseTestData {
  type WarehouseId = String
  type WarehouseClock = Long

  case class WarehouseData(products: Map[WarehouseClock, String]) extends Data {
    override type C = WarehouseClock
    // since products is grow only then we can compute clock from products map
    override val clock: WarehouseClock = if (products.isEmpty) 0L else products.keySet.max
  }

  object WarehouseOps extends DataOps {
    override type D = WarehouseData
    override val ordering: Ordering[WarehouseClock] = Ordering.Long
    override val zero: WarehouseData = WarehouseData(Map.empty)

    override def nextClock(current: WarehouseClock): WarehouseClock =
      System.currentTimeMillis max (current + 1L)

    override def combine(a: WarehouseData, b: WarehouseData): WarehouseData =
      WarehouseData(a.products ++ b.products)

    override def diffFromClock(data: WarehouseData, from: WarehouseClock): WarehouseData =
      WarehouseData(products = data.products.filterKeys(ordering.gt(_, from)))

    override def getRelations(data: WarehouseData): Set[String] =
      data.products.values.toSet
  }

  case class WarehouseEntity(warehouseId: WarehouseId) extends Entity {
    lazy val id: String = "warehouse_" + warehouseId
    val ops = WarehouseOps
  }
}
