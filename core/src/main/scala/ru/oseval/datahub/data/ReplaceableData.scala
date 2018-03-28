package ru.oseval.datahub.data

/**
  * Operations for the data which could be fully replaced by an update
  */
trait ReplaceableDataOps extends DataOps {
  override def combine(a: D, b: D): D =
    if (ordering.gt(a.clock, b.clock)) a else b

  override def diffFromClock(a: D, from: Long): D =
    if (ordering.gt(a.clock, from)) a else zero
}
