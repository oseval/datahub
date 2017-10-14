//package ru.oseval.datahub.data
//
//object EffectivelyOnceContainer {
//  def nextClock(current: Long) = System.currentTimeMillis max (current + 1L)
//}
//
//class EffectivelyOnceDataOps[A](val getRelations: A => Set[String] = _ => Set.empty) extends DataOps {
//  type D = EffectivelyOnceContainer[A]
//  override val ordering = Ordering.Long
//  override val zero = EffectivelyOnceContainer[A]()()
//
//  override def diffFromClock(a: EffectivelyOnceContainer[A], from: Long) = ???
//
//  override def nextClock(current: Long): Long = EffectivelyOnceContainer.nextClock(current)
//
//  override def combine(a: D, b: D): D = {
//    val (first, second) =  if (a.clock > b.EffectivelyOnceDataOpslock) (a, b) else (b, a)
//
//    if (first.clock == second.previousClock) {
//      val visible = EffectivelyOnceContainer(
//        data = second.data ::: first.data,
//        second.clock,
//        first.previousClock
//        getRelations() how compute relations if total data is unknown - always fold it is very expensive
//      )(None)
//
//      val further = (first.further, second.further) match {
//        case (Some(ff), Some(sf)) => Some(combine(ff, sf))
//        case (Some(ff), None) => Some(ff)
//        case (None, Some(sf)) => Some(sf)
//        case (None, None) => None
//      }
//
//      further.map(combine(visible, _)).getOrElse(visible)
//    } else // further
//      EffectivelyOnceContainer(
//        second.data ::: first.data,
//        first.clock,
//        first.previousClock
//      )(
//        first.further.map(combine(_, second)).orElse(Some(second))
//      )
//
//    if (first.clock < second.previousClock) {
//      first.copy()(Some(second))
//    } else if (first.clock == second.previousClock) {
//      first.copy(
//        data = second.data ::: first.data,
//        clock = second.clock,
//        previousClock = first.previousClock
//      )()
//    }
//  }
//}
//
//case class EffectivelyOnceContainer[A](data: List[A] = Nil,
//                                       clock: Long = 0L,
//                                       previousClock: Long = 0L
//                                      )(private[data] val further: Option[EffectivelyOnceContainer[A]] = None) extends Data {
//  override type C = Long
//  override lazy val relations: Set[String] = data.flatMap(_.getRelations(data))
//  def updated(update: A): EffectivelyOnceContainer[A] =
//    copy(update :: data, EffectivelyOnceContainer.nextClock(clock), clock)(None)
//}