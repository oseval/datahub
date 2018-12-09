package ru.oseval.datahub.data

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

case class SetData[+A, Clk](private[data] val added: SortedMap[Clk, A],
                            private[data] val removed: SortedMap[Clk, A]) {
  private lazy val actualMap: SortedMap[Clk, A] = added -- removed.keySet
  lazy val elements: Seq[A] = actualMap.values.toList
  lazy val removedElements: Seq[A] = (removed -- actualMap.keySet).values.toList
  def add[B >: A](el: B, newClock: Clk): SetData[B, Clk] =
    SetData(added + (newClock -> el), removed)

  def remove[B >: A](el: B, newClock: Clk): SetData[B, Clk] =
    SetData(added, removed.updated(newClock, el))
}