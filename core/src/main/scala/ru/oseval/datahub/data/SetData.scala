package ru.oseval.datahub.data

import ru.oseval.datahub.Entity

class SetDataOps[A](_makeId: Any => String) extends DataOps[SetData[A]] {
  override val ordering = new Ordering[String] {
    override def compare(x: String, y: String) = Ordering.Long.compare(x.toLong, y.toLong)
  }
  override val zero = SetData[A](System.currentTimeMillis.toString, "0")(Map.empty, Map.empty, None)

  //  override def combineExactly(a: SetData[A], b: SetData[A]): SetData[A] = {
  //    val (first, second) = if (a.clock.toLong > b.clock.toLong) (b, a) else (a, b)
  //    if (first.clock == second.previousClock) {
  //      val visible = SetData(second.clock, first.previousClock)(
  //        first.underlying ++ second.underlying,
  //        first.removed ++ second.removed,
  //        None
  //      )
  //      val further = (first.further, second.further) match {
  //        case (Some(ff), Some(sf)) => Some(combine(ff, sf))
  //        case (Some(ff), None) => Some(ff)
  //        case (None, Some(sf)) => Some(sf)
  //        case (None, None) => None
  //      }
  //
  //      further.map(combine(visible, _)).getOrElse(visible)
  //    } else SetData(
  //      first.clock, first.previousClock
  //    )(first.underlying, first.removed, first.further.map(combine(_, second)).orElse(Some(second)))
  //  }

  override def combine(a: SetData[A], b: SetData[A]): SetData[A] = {
    val (first, second) = if (a.clock.toLong > b.clock.toLong) (b, a) else (a, b)

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

  override def diffFromClock(a: SetData[A], from: String) =
    SetData(
      ordering.max(from, a.previousClock),
      ordering.max(from, a.clock)
    )(
      a.underlying.filterKeys(c => ordering.gt(c, from)),
      a.underlying.filterKeys(c => ordering.gt(c, from)),
      a.further.map(diffFromClock(_, from))
    )

  override def getRelations(data: SetData[A]): Set[String] = Set.empty

  override def makeId(ownId: Any) = _makeId(ownId)
}

case class SetData[+A](clock: String, previousClock: String)
                      (private[data] val underlying: Map[String, A],
                       private[data] val removed: Map[String, A],
                       private[data] val further: Option[SetData[A]]) extends AtLeastOnceData {
  val elements: Seq[A] = underlying.toList.sortBy(_._1.toLong).map(_._2)
  lazy val isContinious: Boolean = further.isEmpty
  def +[B >: A](el: B): SetData[B] = {
    val newClock = System.currentTimeMillis.toString
    if (clock == newClock) {
      Thread.sleep(1)
      this + el
    }
    else SetData(newClock, clock)(underlying + (newClock -> el), removed, further)
  }

  def drop[B >: A](el: B): SetData[B] = {
    val newClock = System.currentTimeMillis.toString
    SetData(newClock, clock)(underlying, removed.updated(newClock, el), further)
  }
}