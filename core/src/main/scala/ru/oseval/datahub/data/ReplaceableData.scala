package ru.oseval.datahub.data

trait ReplaceableDataOps extends DataOps {
  override def combine(a: D, b: D): D =
    if (ordering.gt(a.clock, b.clock)) a else b
}
