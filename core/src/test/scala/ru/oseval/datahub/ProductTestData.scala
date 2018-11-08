package ru.oseval.datahub

import ru.oseval.datahub.data.{ALODataOps, Data, DataOps}

object ProductTestData {
  type ProductId = Int
  type ProductClock = Long

  case class ProductData(name: String, amount: Int, lastUpdated: Long) extends Data {
    override type C = Long
    override val clock: ProductClock = lastUpdated
  }

  object ProductOpsSimple extends DataOps {
    override type D = ProductData
    override val ordering: Ordering[ProductClock] = Ordering.Long
    override val zero: ProductData = ProductData("", 0, 0L)

    override def nextClock(current: ProductClock): ProductClock =
      System.currentTimeMillis max (current + 1L)

    override def combine(a: ProductData, b: ProductData): ProductData =
      if (ordering.gt(a.clock, b.clock)) a else b

    override def diffFromClock(data: ProductData, from: ProductClock): ProductData = data

    override def getRelations(data: ProductData): (Set[Entity], Set[Entity]) = (Set.empty, Set.empty)
  }

  case object ProductOps extends ALODataOps[ProductOpsSimple.type] {
    override protected val ops: ProductOpsSimple.type = ProductOpsSimple
  }

  case class ProductEntity(productId: Int) extends Entity {
    val kind: String = "product"
    lazy val id: String = kind + "_" + productId
    val ops = ProductOps
  }
}