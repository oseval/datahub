package ru.oseval.datahub

import org.scalatest.{FlatSpecLike, Matchers}
import ru.oseval.datahub.data.{ClockInt, SetDataOps}

class SetDataSpec extends FlatSpecLike with Matchers {
  behavior of "SetData"

  private implicit val cint: ClockInt[Long] = ClockInt(0L, System.currentTimeMillis)
  private val zeroData = SetDataOps.zero[Int, Long]
  private val seedData = 1 to 60

  it should "add elements" in {
    val data = seedData.grouped(20).map(_.foldLeft((System.currentTimeMillis, zeroData)) { case ((ctime, d), i) =>
      val nextTime = System.currentTimeMillis max (ctime + 1L)
      (nextTime, d.add(i)(ClockInt(cur = nextTime)))
    }._2).toList

    val res = (data ++ data).permutations.foldLeft(zeroData)((d, s) =>
      s.fold(d)(SetDataOps.combine)
    )
    res.elements shouldBe seedData.toList
  }

  it should "add and remove elements" in {
    val data = seedData.grouped(20).map(_.foldLeft((System.currentTimeMillis, zeroData)) { case ((ctime, d), i) =>
      val nextTime = System.currentTimeMillis max (ctime + 1L)
      if (i % 3 == 1) (nextTime, d.drop(i - 1)(ClockInt(cur = nextTime)))
      else (nextTime, d.add(i)(ClockInt(cur = nextTime)))
    }._2).toList
    val res = (data ++ data).permutations.foldLeft(zeroData)((r, s) => s.fold(zeroData)(SetDataOps.combine))
    res.elements shouldBe seedData.filterNot(_ % 3 == 1).toList
  }
}
