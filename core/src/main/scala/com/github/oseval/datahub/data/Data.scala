package com.github.oseval.datahub.data

import com.github.oseval.datahub.{Entity, LocalEntityFacade}

/**
  * Idempotent (due to [[Data.clock]] and commutative (due to [[DataOps.ordering]]) data model.
  */
trait Data { self =>
  type C
  val clock: C
}

/**
  * When you want to be sure that your data will reach a destination.
  * Such data is data which sends by partial updates (elements of a list for example)
  * To achieve this guarantee and found gaps it has [[AtLeastOnceData.previousClock]].
  * Data integrity could be checked by [[AtLeastOnceData.isSolid]] param.
  */
trait AtLeastOnceData extends Data {
  val previousClock: C
  val isSolid: Boolean
}

abstract class DataOps {
  type D <: Data
  val kind: String = getClass.getName

  val ordering: Ordering[D#C]
  /**
   * Data which is initial state for all such entities
   */
  val zero: D

  /**
    * Combines two data objects to one
    * @param a
    * @param b
    * @return
    */
  def combine(a: D, b: D): D =
    if (ordering.gt(a.clock, b.clock)) a else b

  /**
    * Computes diff between `a` and older state with a `from` id
    * @param a
    * @param from
    * @return
    */
  def diffFromClock(a: D, from: D#C): D =
    if (ordering.gt(a.clock, from)) a else zero

  def nextClock(current: D#C): D#C

  def matchData(data: Data): Option[D] =
    if (zero.getClass.isAssignableFrom(data.getClass))
      Option(zero.getClass.cast(data))
    else
      None

  def matchClock(clock: Any): Option[D#C] =
    if (clock.getClass == zero.clock.getClass) Some(clock.asInstanceOf[D#C]) else None

  def approveRelation(data: D, relationId: String): Boolean = true

  def widen[T >: this.type <: DataOps]: T = this
}

trait OpsWithRelations[D] {
  // TODO: store as SetData?
  def getRelations(data: D): (Set[Entity], Set[Entity])
}

object InferredOps {
  trait ImplicitClockBehavior[C] {
    val ordering: Ordering[C]
    def nextClock(current: C): C
  }
  implicit val timeClockBehavior: ImplicitClockBehavior[Long] = new ImplicitClockBehavior[Long] {
    val ordering: Ordering[Long] = Ordering.Long
    def nextClock(current: Long): Long = System.currentTimeMillis max (current + 1L)
  }
  abstract class InferredOps[Dt <: Data](z: Dt, k: String, behavior: ImplicitClockBehavior[Dt#C]) extends DataOps {
    override type D = Dt
    override val kind: String = k
    override val zero: D = z
    override val ordering: Ordering[D#C] = behavior.ordering
    override def nextClock(current: Dt#C): D#C = behavior.nextClock(current)
  }
  def apply[Dt <: Data](z: Dt)(implicit behavior: ImplicitClockBehavior[Dt#C]): InferredOps[Dt] =
    apply(z, z.getClass.getName)
  def apply[Dt <: Data](empty: Dt, kind: String)(implicit behavior: ImplicitClockBehavior[Dt#C]): InferredOps[Dt] =
    new InferredOps[Dt](empty, kind, behavior) {}
  def apply[Dt <: Data](empty: Dt, kind: String, relations: Dt => (Set[Entity], Set[Entity]))
                       (implicit behavior: ImplicitClockBehavior[Dt#C]): InferredOps[Dt] =
    new InferredOps[Dt](empty, kind, behavior) with OpsWithRelations[Dt] {
      override def getRelations(data: D): (Set[Entity], Set[Entity]) = relations(data)
    }
}

case class ClockInt[C](cur: C, start: C)

case class InvalidDataException(message: String) extends RuntimeException(message)


//       ai     alo    eff
//
//a                    +      prevId
//
//c      +      +      +      order
//
//i      +      +      +      id
