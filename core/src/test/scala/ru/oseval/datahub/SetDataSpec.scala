package ru.oseval.datahub

import org.scalatest.{FlatSpecLike, Matchers}
import ru.oseval.datahub.data.{SetData, SetDataOps}

class SetDataSpec extends FlatSpecLike with Matchers {
  behavior of "SetData"

  val seedData = 1 to 100
  val data = seedData.grouped(20).map(_.foldLeft(ops.zero)(_ + _))

  it should "add elements" in {
    val a = seedData.grouped(20).toList.head.toList
    println((ops.zero + 1 + 2 + 3 + 4 elements))

    val res = data.toList.permutations.foldLeft(ops.zero)((r, s) => s.fold(ops.zero)(ops.combine))
    res.elements shouldBe seedData.toList
  }
}
