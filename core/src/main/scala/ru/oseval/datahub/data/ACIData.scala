package ru.oseval.datahub.data

import ru.oseval.datahub.{Entity, EntityFacade}

/**
  * Data which are associative and idempotent
  */
object ACIDataOps {
  def nextClock(current: Long): Long =
    System.currentTimeMillis max (current + 1L)
}

// TODO: actually this should be Replaceable Ops
abstract class ACIDataOps[A](relations: A => (Set[Entity], Set[Entity]) =
                             (_: A) => (Set.empty[Entity], Set.empty[Entity]),
                             forcedSubscribers: A => Set[EntityFacade] = (_: A) => Set.empty[EntityFacade])
  extends DataOps {

  override type D = ACIData[A]
  override val ordering: Ordering[Long] = Ordering.Long
  override val zero: D = ACIData()

  override def combine(a: D, b: D): D

  override def diffFromClock(a: D, from: Long): D

  override def getRelations(data: D): (Set[Entity], Set[Entity]) =
    data.data.map(relations) getOrElse (Set.empty, Set.empty)

  override def getForcedSubscribers(data: D): Set[EntityFacade] =
    data.data.map(forcedSubscribers) getOrElse Set.empty

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
