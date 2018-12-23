package ru.oseval.datahub.domain

import ru.oseval.datahub.Entity
import ru.oseval.datahub.data.InferredOps.{InferredOps}
import ru.oseval.datahub.data.{Data, InferredOps}

object ProductTestData {
  type ProductId = Int
  type ProductClock = Long

  case class ProductData(name: String, amount: Int, lastUpdated: ProductClock) extends Data {
    override type C = Long
    override val clock: ProductClock = lastUpdated
  }

  case object ProductOps extends InferredOps[ProductData](
    ProductData("", 0, 0L), "producat", InferredOps.timeClockBehavior
  )

  case class ProductEntity(productId: Int) extends Entity {
    lazy val id: String = ProductOps.kind + "_" + productId
    val ops = ProductOps
  }
}
