package com.github.oseval.datahub

import org.scalatest.{FlatSpecLike, Matchers}
import com.github.oseval.datahub.data.{ClockInt, SetDataOps}

class SetDataSpec extends FlatSpecLike with Matchers {
  behavior of "SetData"

  private implicit val cint: ClockInt[Int] = ClockInt(0, 0)
  private val zeroData = SetDataOps.zero[Int, Int]
  private val seedData = 1 to 60

  it should "add elements" in {
    val data = seedData.grouped(20).zipWithIndex.map { case (elms, i) =>
      elms.foldLeft((i*20, zeroData)) { case ((ctime, d), elm) =>
        val nextTime = ctime + 1
        (nextTime, d.add(elm, nextTime))
      }._2
    }.toList

    val res = (data ++ data).permutations.foldLeft(zeroData)((d, s) =>
      s.fold(d)(_ add _)
    )
    res.elements shouldBe seedData.toList
  }

  it should "add and remove elements" in {
    val data = seedData.grouped(20).zipWithIndex.map { case (elms, i) =>
      elms.foldLeft((i*20, zeroData)) { case ((ctime, d), elm) =>
        val nextTime = ctime + 1
        if (elm % 3 == 1) (nextTime, d.remove(elm - 1, nextTime))
        else (nextTime, d.add(elm, nextTime))
      }._2
    }.toList

    val res = (data ++ data).permutations.foldLeft(zeroData)((r, s) => s.fold(zeroData)(_ add _))
    res.elements shouldBe seedData.filterNot(_ % 3 == 1).toList
  }
}
