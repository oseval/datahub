package ru.oseval.datahub

import ru.oseval.datahub.data.{Data, DataOps}

object ProductTestData {
  type ProductId = Int
  type ProductClock = Long

  case class ProductData(name: String, amount: Int, lastUpdated: Long) extends Data {
    override type C = Long
    override val clock: ProductClock = lastUpdated
  }

  object ProductOps extends DataOps {
    override type D = ProductData
    override val ordering: Ordering[ProductClock] = Ordering.Long
    override val zero: ProductData = ProductData("", 0, 0L)

    override def combine(a: ProductData, b: ProductData): ProductData =
      if (ordering.gt(a.clock, b.clock)) a else b

    override def diffFromClock(data: ProductData, from: ProductClock): ProductData = data

    override def getRelations(data: ProductData): Set[String] = Set.empty
  }

  case class ProductEntity(ownId: Int) extends Entity {
    val ops = ProductOps
    override type ID = Int
    override def makeId(ownId: Int): String = "product_" + ownId
  }
}