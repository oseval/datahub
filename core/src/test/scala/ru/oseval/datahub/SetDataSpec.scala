package ru.oseval.datahub

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpecLike, Matchers}
import ru.oseval.datahub.data.{SetData, SetDataOps}

class SetDataSpec extends FlatSpecLike with GeneratorDrivenPropertyChecks with Matchers {
  behavior of "SetData"

  val ops = new SetDataOps[Int]("testset_" + _)

  val seedData = 1 to 1000
  val data = seedData.grouped(20).map(_.foldLeft(ops.zero)(_ + _))

  it should "add elemements" in {
    val res = data.toList.permutations.foldLeft(ops.zero)((r, s) => s.fold(ops.zero)(ops.combine))
    res.elements shouldBe seedData.toList
  }
}
