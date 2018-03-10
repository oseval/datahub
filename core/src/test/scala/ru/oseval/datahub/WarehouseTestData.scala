package ru.oseval.datahub

import ru.oseval.datahub.ProductTestData.ProductEntity
import ru.oseval.datahub.data._

object WarehouseTestData {
  type WarehouseId = String

  object WarehouseOps extends ALODataOps[Int](pid => (Set.empty, Set(ProductEntity(pid))), _ => Set.empty) {
    override def createFacadeFromEntityId(entityId: String): Option[EntityFacade] = None
  }

  case class WarehouseEntity(warehouseId: WarehouseId) extends Entity {
    val kind: String = "warehouse"
    lazy val id: String = kind + "_" + warehouseId
    val ops = WarehouseOps
    override val untrustedKinds: Set[String] = Set(ProductTestData.ProductOps.kind)
  }
}
