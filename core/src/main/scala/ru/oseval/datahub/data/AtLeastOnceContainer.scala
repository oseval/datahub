package ru.oseval.datahub.data
//
//case class AtLeastOnceContainer[D <: Data](clock: D#C,
//                                           previousClock: D#C,
//                                           data: D
//                                          )(further: Option[AtLeastOnceContainer[D]] = None) extends AtLeastOnceData {
//  lazy val isSolid: Boolean = further.isEmpty
//  def updated(upd: D => D, tick: D#C => D#C): AtLeastOnceContainer[D] =
//    copy(tick(clock), previousClock, upd(data))()
//
////  def combine
//}
