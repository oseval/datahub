package ru.oseval.datahub.data

import ru.oseval.datahub.{Entity, EntityFacade}

import scala.collection.SortedMap

/**
  * This wrapper intended to add the at-least-one delivery control ability to data.
  * Useful in case of compound data where each pieces are ACID data and have a lot of such pieces
  * replicated separately. For example sequence of updates of some big data.
  * An A must be associative and idempotent.
  *
  * @param ops ops for wrapping
  * @tparam A
  */
abstract class ALODataOps[A <: Data, AO <: DataOps](ops: AO)
                                                   (implicit ev1: A =:= ops.D,
                                                    ev2: ops.D =:= A,
                                                    evC: A#C =:= ops.D#C) extends DataOps {
  type D = ALOData[A]
  override val ordering = Ordering.by(evC)(ops.ordering)
  private val zeroA: A = ev2(ops.zero)
  override val zero: ALOData[A] = ALOData(None, zeroA.clock, zeroA.clock, None)

  override def getRelations(data: D): (Set[Entity], Set[Entity]) =
    data.data match {
      case Some(d) => ops.getRelations(ev1(d))
      case None => (Set.empty[Entity], Set.empty[Entity])
    }

  // TODO: can't be true if it a partial data - for local storage only. Restrict access to it.
  override def diffFromClock(a: ALOData[A], from: A#C): ALOData[A] =
    if (a.isSolid)
      ALOData(
        a.data.map(ops.diffFromClock(_, from)),
        a.clock,
        if (ordering.gteq(a.clock, from)) from else a.clock,
        None
      )
    else
      throw InvalidDataException("Diff can't be done on a partial data")

  private def combineData: (Option[A], Option[A]) => Option[A] = {
    case (Some(f), Some(s)) => Some(ops.combine(f, s))
    case (Some(f), None) => Some(f)
    case (None, s) => s
  }
  override def combine(a: D, b: D): D = {
    val (first, second) =  if (ordering.gt(a.clock, b.clock)) (b, a) else (a, b)

//    | --- | |---|
//
//    | --- |
//       | --- |
//
//      | --- |
//    | -------- |

    if (ordering.gteq(first.clock, second.previousClock)) {
      if (ordering.gteq(first.previousClock, second.previousClock)) second
      else {
        val visible = ALOData[A](
          data = combineData(first.data, second.data),
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
        combineData(first.data, second.data),
        first.clock,
        first.previousClock,
        first.further.map(combine(_, second)).orElse(Some(second))
      )
  }
}

object ALOData {
  def apply[A <: Data](data: A)(implicit clockInt: ClockInt[A#C]): ALOData[A] =
    ALOData(Some(data), clockInt.cur, clockInt.start)
}

case class ALOData[A <: Data](data: Option[A],
                              clock: A#C,
                              previousClock: A#C,
                              private[data] val further: Option[ALOData[A]] = None
                             ) extends AtLeastOnceData {
  override type C = A#C
  val isSolid: Boolean = further.isEmpty
  def updated(updated: A, newClock: A#C): ALOData[A] =
    copy(data = Some(updated), clock = newClock)
}