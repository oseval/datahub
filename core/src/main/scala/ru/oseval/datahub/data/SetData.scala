package ru.oseval.datahub.data

object SetDataOps {
  def zero[A, C](clock: C, previosClock: C) =
    SetData[A, C](clock, previosClock)(Map.empty, Map.empty, None)

  def combine[A, C](a: SetData[A, C], b: SetData[A, C])
                            (implicit ordering: Ordering[C]): SetData[A, C] = {
    val (first, second) = if (ordering.gt(a.clock, b.clock)) (b, a) else (a, b)

    if (first.clock == second.previousClock) {
      val visible = SetData(second.clock, first.previousClock)(
        first.underlying ++ second.underlying,
        first.removed ++ second.removed,
        None
      )

      val further = (first.further, second.further) match {
        case (Some(ff), Some(sf)) => Some(combine(ff, sf))
        case (Some(ff), None) => Some(ff)
        case (None, Some(sf)) => Some(sf)
        case (None, None) => None
      }


      further.map(combine(visible, _)).getOrElse(visible)
    } else
      SetData(
        first.clock,
        first.previousClock
      )(
        first.underlying ++ second.underlying,
        first.removed ++ second.removed,
        first.further.map(combine(_, second)).orElse(Some(second))
      )
  }

  def diffFromClock[A, C](a: SetData[A, C], from: C)(implicit ordering: Ordering[C]): SetData[A, C] =
    SetData(
      ordering.max(from, a.previousClock),
      ordering.max(from, a.clock)
    )(
      a.underlying.filterKeys(c => ordering.gt(c, from)),
      a.underlying.filterKeys(c => ordering.gt(c, from)),
      a.further.map(diffFromClock(_, from))
    )
}

case class SetData[+A, Clk](clock: Clk, previousClock: Clk)
                           (private[data] val underlying: Map[Clk, A],
                            private[data] val removed: Map[Clk, A],
                            private[data] val further: Option[SetData[A, Clk]]) extends AtLeastOnceData {
  type C = Clk
  val elements: Seq[A] = underlying.toList.map(_._2)
  lazy val isContinious: Boolean = further.isEmpty
  def add[B >: A](el: B, newClock: Clk): SetData[B, Clk] = {
    SetData(newClock, clock)(underlying + (newClock -> el), removed, further)
  }

  def drop[B >: A](el: B, newClock: Clk): SetData[B, Clk] = {
    SetData(newClock, clock)(underlying, removed.updated(newClock, el), further)
  }
}