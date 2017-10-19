package ru.oseval.datahub.data

object ACIDataOps {
  def nextClock(current: Long): Long =
    System.currentTimeMillis max (current + 1L)
}

abstract class ACIDataOps[A](relations: A => Set[String] = (_: A) => Set.empty[String]) extends DataOps {
  override type D = ACIData[A]
  override val ordering: Ordering[Long] = Ordering.Long
  override val zero: D = ACIData()

  override def combine(a: D, b: D): D =
    if (a.clock > b.clock) a else b

  override def diffFromClock(a: D, from: Long): D =
    if (a.clock > from) a else zero

  override def getRelations(data: D): Set[String] = data.data.toSet.flatMap(relations)

  override def nextClock(current: Long): Long = ACIDataOps.nextClock(current)
}

object ACIData {
  def apply[A](data: A, clock: Long): ACIData[A] =
    ACIData(Some(data), clock)
}

case class ACIData[A](data: Option[A] = None, clock: Long = 0L) extends Data {
  override type C = Long
  // TODO: implicit clockint of generic type
//  def updated(update: A): ACIData[A] = ACIData(Some(update), ACIDataOps.nextClock(clock))
}
