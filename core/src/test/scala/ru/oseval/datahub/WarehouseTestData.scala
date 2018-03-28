package ru.oseval.datahub

import ru.oseval.datahub.ProductTestData.ProductEntity
import ru.oseval.datahub.data._

object WarehouseTestData {
  type WarehouseId = String

  object WarehouseOps extends CumulativeDataOps[Int](pid => (Set(ProductEntity(pid)), Set.empty), _ => Set.empty) {
    override val kind: String = "warehouse"
    override def createFacadeFromEntityId(entityId: String): Option[EntityFacade] = None
  }

  case class WarehouseEntity(warehouseId: WarehouseId) extends Entity {
    lazy val id: String = WarehouseOps.kind + "_" + warehouseId
    val ops = WarehouseOps
    override val untrustedKinds: Set[String] = Set(ProductTestData.ProductOps.kind)
  }
}
