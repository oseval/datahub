package ru.oseval.datahub.data

object ACIDataOps extends DataOps {
  override type D = ACIData[_]
  override val ordering: Ordering[Long] = Ordering.Long
  override val zero: D = ACIData()

  override def combine(a: D, b: D): D =
    if (a.clock > b.clock) a else b

  override def diffFromClock(a: D, from: Long): D =
    if (a.clock > from) a else zero

  override def getRelations(data: D): Set[String] = Set.empty

  override def nextClock(current: Long): Long =
    System.currentTimeMillis max (current + 1L)
}

object ACIData {
  def apply[A](data: A): ACIData[A] = ACIData(Some(data), System.currentTimeMillis)
}

case class ACIData[A](data: Option[A] = None, clock: Long = 0L) extends Data {
  override type C = Long
  def updated(update: A): ACIData[A] = copy(Some(update), ACIDataOps.nextClock(clock))
}
