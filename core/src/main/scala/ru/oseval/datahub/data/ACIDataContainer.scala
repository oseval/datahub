package ru.oseval.datahub.data

object ACIDataOps extends DataOps {
  override type D = ACIDataContainer[_]
  override val ordering: Ordering[Long] = Ordering.Long
  override val zero: D = ACIDataContainer()

  override def combine(a: D, b: D): D =
    if (a.clock > b.clock) a else b

  override def diffFromClock(a: D, from: Long): D =
    if (a.clock > from) a else zero

  override def nextClock(current: Long) =
    System.nanoTime max (current + 1L)

  override def getRelations(data: D): Set[String] = Set.empty
}

case class ACIDataContainer[A](data: Option[A] = None, clock: Long = 0L) extends Data {
  override type C = Long
  def updated(update: A): ACIDataContainer[A] = copy(Some(update), ACIDataOps.nextClock(clock))
}
