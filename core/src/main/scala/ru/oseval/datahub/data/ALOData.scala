package ru.oseval.datahub.data

object ALOData {
  def nextClock(current: Long) = System.currentTimeMillis max (current + 1L)
}

abstract class ALODataOps[A](val getRelations: A => Set[String] = (_: A) => Set.empty) extends DataOps {
  type D = ALOData[A]
  override val ordering = Ordering.Long
  override val zero: ALOData[A] = ALOData[A]()()

  override def diffFromClock(a: ALOData[A], from: Long) =
    ALOData(
      a.data.filterKeys(_ > from),
      a.clock,
      from
    )(a.further)

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
      val visible =
        if (first.previousClock >= second.previousClock) second
        else ALOData(
          data = second.data ++ first.data,
          second.clock,
          first.previousClock
        )(None)

      val further = (first.further, second.further) match {
        case (Some(ff), Some(sf)) => Some(combine(ff, sf))
        case (Some(ff), None) => Some(ff)
        case (None, Some(sf)) => Some(sf)
        case (None, None) => None
      }

      further.map(combine(visible, _)).getOrElse(visible)
    } else // further
      ALOData(
        second.data ++ first.data,
        first.clock,
        first.previousClock
      )(
        first.further.map(combine(_, second)).orElse(Some(second))
      )
  }
}

case class ALOData[A](data: Map[Long, A] = Map.empty[Long, A],
                      clock: Long = 0L,
                      previousClock: Long = 0L
                     )(private[data] val further: Option[ALOData[A]] = None)
  extends AtLeastOnceData {
  override type C = Long
  val isSolid: Boolean = further.isEmpty
  lazy val elements: Seq[A] = data.values.toList
  def updated(update: A): ALOData[A] = {
    val newClock = ALOData.nextClock(clock)
    copy(data.updated(newClock, update), newClock, clock)(None)
  }
}