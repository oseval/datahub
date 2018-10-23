package ru.oseval.datahub.data

import scala.collection.SortedMap

/**
  * Data to help operating with collections inside at-least-once and eff-once Data wrappers.
  * This class adds to collection "diffFromClock" ability.
  */
object SetDataOps {
  def zero[A, C](implicit clockInt: ClockInt[C], ordering: Ordering[C]) =
    SetData[A, C](SortedMap.empty, SortedMap.empty, None)

  def diffFromClock[A, C](a: SetData[A, C], from: C)(implicit ordering: Ordering[C]): SetData[A, C] =
    SetData(
      a.underlying.filterKeys(c => ordering.gt(c, from)),
      a.removed.filterKeys(c => ordering.gt(c, from)),
    )
}

case class SetData[+A, Clk](private[data] val underlying: SortedMap[Clk, A],
                            private[data] val removed: SortedMap[Clk, A]) {
  type C = Clk
  lazy val elements: Seq[A] = (underlying -- removed.keySet).values.toList
  def add[B >: A](el: B)(implicit newCint: ClockInt[Clk]): SetData[B, Clk] =
    SetData(underlying + (newCint.cur -> el), removed)

  def remove[B >: A](el: B)(implicit newCint: ClockInt[Clk]): SetData[B, Clk] =
    SetData(underlying, removed.updated(newCint.cur, el))
}