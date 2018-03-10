package ru.oseval.datahub.data

import ru.oseval.datahub.{Entity, EntityFacade}

import scala.collection.SortedMap

abstract class ALODataOps[A](relations: A => (Set[Entity], Set[Entity]) =
                             (_: A) => (Set.empty, Set.empty),
                             forcedSubscribers: A => Set[EntityFacade] = (_: A) => Set.empty) extends DataOps {
  type D = ALOData[A]
  override val ordering: Ordering.Long.type = Ordering.Long
  override val zero: ALOData[A] = ALOData[A]()

  override def getRelations(data: D): (Set[Entity], Set[Entity]) =
    data.data.values.foldLeft((Set.empty[Entity], Set.empty[Entity])) { case ((add, rem), d) =>
      val (a, r) = relations(d)
      (add ++ a) -> (r ++ rem)
    }
  override def getForcedSubscribers(data: D): Set[EntityFacade] =
    data.data.values.foldLeft(Set.empty[EntityFacade])((f, d) => f ++ forcedSubscribers(d))

  override def diffFromClock(a: ALOData[A], from: Long): ALOData[A] =
    ALOData(a.data.filterKeys(_ > from), a.clock, from, a.further)

  override def nextClock(current: Long): Long = ALOData.nextClock(current)

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
        val visible = ALOData(
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
      ALOData(
        second.data ++ first.data,
        first.clock,
        first.previousClock,
        first.further.map(combine(_, second)).orElse(Some(second))
      )
  }
}

object ALOData {
  def apply[A](el: A)(implicit clockInt: ClockInt[Long]): ALOData[A] =
    ALOData(SortedMap(clockInt.cur -> el), clockInt.cur, clockInt.prev, None)
  def nextClock(current: Long): Long = System.currentTimeMillis max (current + 1L)
}

case class ALOData[A](data: SortedMap[Long, A] = SortedMap.empty[Long, A],
                      clock: Long = System.currentTimeMillis,
                      previousClock: Long = 0L,
                      private[data] val further: Option[ALOData[A]] = None
                     ) extends AtLeastOnceData {
  override type C = Long
  val isSolid: Boolean = further.isEmpty
  lazy val elements: Seq[A] = data.values.toList
  def updated(update: A, newClock: Long): ALOData[A] = {
    copy(data.updated(newClock, update), newClock, clock, None)
  }
}