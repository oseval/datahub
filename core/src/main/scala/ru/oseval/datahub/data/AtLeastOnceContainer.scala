package ru.oseval.datahub.data

//case class AtLeastOnceContainer[D <: Data](clock: D#C,
//                                           previousClock: D#C,
//                                           data: D,
//                                           tick: D#C => D#C,
//                                           ops: DataOps
//                                          )(further: Option[AtLeastOnceContainer[D]] = None) extends AtLeastOnceData {
//  lazy val isSolid: Boolean = further.isEmpty
//  def updated(upd: D => D): AtLeastOnceContainer[D] =
//    copy(tick(clock), previousClock, upd(data))()
//
//  def combine(a: AtLeastOnceContainer[D]) =
//    a.data
//}