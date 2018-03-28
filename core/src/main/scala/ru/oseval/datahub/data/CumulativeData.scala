package ru.oseval.datahub.data

import ru.oseval.datahub.{Entity, EntityFacade}

import scala.collection.SortedMap

abstract class CumulativeDataOps[A](relations: A => (Set[Entity], Set[Entity]) =
                                    (_: A) => (Set.empty, Set.empty),
                                    forcedSubscribers: A => Set[EntityFacade] = (_: A) => Set.empty) extends DataOps {
  type D = CumulativeData[A]
  override val ordering: Ordering.Long.type = Ordering.Long
  override val zero: CumulativeData[A] = CumulativeData[A]()

  override def getRelations(data: D): (Set[Entity], Set[Entity]) =
    data.data.values.foldLeft((Set.empty[Entity], Set.empty[Entity])) { case ((add, rem), d) =>
      val (a, r) = relations(d)
      (add ++ a) -> (r ++ rem)
    }
  override def getForcedSubscribers(data: D): Set[EntityFacade] =
    data.data.values.foldLeft(Set.empty[EntityFacade])((f, d) => f ++ forcedSubscribers(d))

  override def diffFromClock(a: CumulativeData[A], from: Long): CumulativeData[A] =
    CumulativeData(a.data.filterKeys(_ > from), a.clock, from, a.further)

  override def nextClock(current: Long): Long = CumulativeData.nextClock(current)

  override def combine(a: D, b: D): D = {
    val (first, second) =  if (a.clock > b.clock) (b, a) else (a, b)

//    | --- | |---|
//
//    | --- |
//       | --- |
//
//      | --- |
//    | -------- |

    if (first.clock >= second.previousClock) {
      if (first.previousClock >= second.previousClock) second
      else {
        val visible = CumulativeData(
          data = second.data ++ first.data,
          second.clock,
          first.previousClock
        )

        val further = (first.further, second.further) match {
          case (Some(ff), Some(sf)) => Some(combine(ff, sf))
          case (Some(ff), None) => Some(ff)
          case (None, Some(sf)) => Some(sf)
          case (None, None) => None
        }

        further.map(combine(visible, _)).getOrElse(visible)
      }
    } else // further
      CumulativeData(
        second.data ++ first.data,
        first.clock,
        first.previousClock,
        first.further.map(combine(_, second)).orElse(Some(second))
      )
  }
}

object CumulativeData {
  def apply[A](el: A)(implicit clockInt: ClockInt[Long]): CumulativeData[A] =
    CumulativeData(SortedMap(clockInt.cur -> el), clockInt.cur, clockInt.prev, None)
  def nextClock(current: Long): Long = System.currentTimeMillis max (current + 1L)
}

case class CumulativeData[A](data: SortedMap[Long, A] = SortedMap.empty[Long, A],
                             clock: Long = System.currentTimeMillis,
                             previousClock: Long = 0L,
                             private[data] val further: Option[CumulativeData[A]] = None
                            ) extends AtLeastOnceData {
  override type C = Long
  val isSolid: Boolean = further.isEmpty
  lazy val elements: Seq[A] = data.values.toList
  def updated(update: A, newClock: Long): CumulativeData[A] = {
    copy(data.updated(newClock, update), newClock, clock, None)
  }
}