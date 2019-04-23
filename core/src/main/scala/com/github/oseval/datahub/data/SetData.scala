package com.github.oseval.datahub.data

import scala.collection.SortedMap

/**
  * Data to help operating with collections inside at-least-once and eff-once Data wrappers.
  * This class adds to collection "diffFromClock" ability.
  */
object SetDataOps {
  def zero[A, C](implicit ordering: Ordering[C]) =
    SetData[A, C](SortedMap.empty, SortedMap.empty)

  def diffFromClock[A, C](a: SetData[A, C], from: C)(implicit ordering: Ordering[C]): SetData[A, C] =
    SetData(
      a.added.filterKeys(c => ordering.gt(c, from)),
      a.removed.filterKeys(c => ordering.gt(c, from)),
    )
}

object SetData {
  def one[A, C](element: A, clock: C)(implicit ordering: Ordering[C]) =
    new SetData(SortedMap(clock -> element), SortedMap.empty)

  def apply[A, C](elements: A*)(clock: C)(increase: C => C)(implicit ordering: Ordering[C]) =
    new SetData(elements.foldLeft(SortedMap.empty[C, A] -> clock) { case ((m, clk), el) =>
      m.updated(clk, el) -> increase(clk)
    }._1, SortedMap.empty[C, A])
}

case class SetData[+A, Clk](private[data] val added: SortedMap[Clk, A],
                            private[data] val removed: SortedMap[Clk, A]) {
  private lazy val actualMap: SortedMap[Clk, A] = added -- removed.keySet
  lazy val lastClockOpt = (added.lastOption.map(_._1), removed.lastOption.map(_._1)) match {
    case (None, clkOpt) => clkOpt
    case (clkOpt, None) => clkOpt
    case (Some(clk1), Some(clk2)) => Some(added.ordering.max(clk1, clk2))
  }
  lazy val elements: Seq[A] = actualMap.values.toList
  lazy val removedElements: Seq[A] = (removed -- actualMap.keySet).values.toList
  def add[B >: A](el: B, newClock: Clk): SetData[B, Clk] =
    SetData(added + (newClock -> el), removed)

  def add[B >: A](sdata: SetData[B, Clk]): SetData[B, Clk] = {
    val allAdded =  (added -- sdata.removed.keySet) ++ (sdata.added -- removed.keySet)
    val allRemoved =  (removed -- sdata.added.keySet) ++ (sdata.removed -- added.keySet)
    SetData(allAdded, allRemoved)
  }

  def remove[B >: A](el: B, newClock: Clk): SetData[B, Clk] =
    SetData(added, removed.updated(newClock, el))
}
