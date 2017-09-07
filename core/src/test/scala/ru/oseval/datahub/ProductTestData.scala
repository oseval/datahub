package ru.oseval.datahub

object ProductTestData {
  type ProductId = String
  type ProductClock = String

  case class ProductData(name: String, amount: Int, lastUpdated: Long) extends Data { self ⇒
    override val clock: ProductClock = lastUpdated.toString
  }

  object ProductOps extends DataOps[ProductData] {
    override val ordering: Ordering[ProductClock] = Data.timestampOrdering
    override val zero: ProductData = ProductData("", 0, 0L)

    override def combine(a: ProductData, b: ProductData): ProductData =
      if (ordering.gt(a.clock, b.clock)) a else b

    override def diffFromClock(data: ProductData, from: ProductClock): ProductData = data

    override def getRelations(data: ProductData): Set[String] = Set.empty

    override def makeId(ownId: Any): String = "product_" + ownId
  }

  case class ProductEntity(ownId: String) extends Entity {
    override type D = ProductData
    val ops = ProductOps
  }
}