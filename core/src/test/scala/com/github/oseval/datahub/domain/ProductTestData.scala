package com.github.oseval.datahub.domain

import com.github.oseval.datahub.Entity
import com.github.oseval.datahub.data.InferredOps.{InferredOps}
import com.github.oseval.datahub.data.{Data, InferredOps}

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
