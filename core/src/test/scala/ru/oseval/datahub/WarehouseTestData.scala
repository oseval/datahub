package ru.oseval.datahub

import ru.oseval.datahub.data._

object WarehouseTestData {
  type WarehouseId = String

  object WarehouseOps extends ALODataOps[String](Set(_))

  case class WarehouseEntity(warehouseId: WarehouseId) extends Entity {
    lazy val id: String = "warehouse_" + warehouseId
    val ops = WarehouseOps
  }
}
