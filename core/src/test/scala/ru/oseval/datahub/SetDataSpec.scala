package ru.oseval.datahub

import org.scalatest.{FlatSpecLike, Matchers}
import ru.oseval.datahub.data.{ClockInt, SetDataOps}

class SetDataSpec extends FlatSpecLike with Matchers {
  behavior of "SetData"

  implicit val cint = ClockInt(0L, System.nanoTime)
  val zeroData = SetDataOps.zero[Int, Long]
  val seedData = 1 to 60

  it should "add elements" in {
    val data = seedData.grouped(20).map(_.foldLeft(zeroData)((data, i) => data.add(i)(System.nanoTime))).toList
    val res = (data ++ data).permutations.foldLeft(zeroData)((r, s) => s.fold(zeroData)(SetDataOps.combine))
    res.elements shouldBe seedData.toList
  }

  it should "add and remove elements" in {
    val data = seedData.grouped(20).map(_.foldLeft(zeroData)((data, i) =>
      if (i % 3 == 1) data.drop(i - 1)(System.nanoTime) else data.add(i)(System.nanoTime)
    )).toList
    val res = (data ++ data).permutations.foldLeft(zeroData)((r, s) => s.fold(zeroData)(SetDataOps.combine))
    res.elements shouldBe seedData.filterNot(_ % 3 == 1).toList
  }
}
