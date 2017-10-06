package ru.oseval.datahub.data

/**
  * Idempotent (due to [[Data.clock]] and commutative (due to [[DataOps.ordering]]) data model.
  */
trait Data { self =>
  type C
  val clock: C
  /**
    * Entity ids which related to the specified data
    */
  val relations: Set[String] = Set.empty
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

/**
  * Any data which must apply updates continually (without gaps).
  */
trait ContinuedData extends AtLeastOnceData

/**
  * If your data is compound from some other datas then you can use this trait to automate data flow
  */
trait CompoundData extends AtLeastOnceData {
  protected val children: Set[Data]
  override lazy val isSolid: Boolean = children.forall { case d: AtLeastOnceData => d.isSolid case _ => true }
}

abstract class DataOps {
  type D <: Data

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
  def combine(a: D, b: D): D

  /**
    * Computes diff between `a` and older state with a `from` id
    * @param a
    * @param from
    * @return
    */
  def diffFromClock(a: D, from: D#C): D

  def nextClock(current: D#C): D#C

  def matchData(data: Data): Option[D] =
    if (zero.getClass.isAssignableFrom(data.getClass))
      Option(zero.getClass.cast(data))
    else
      None

  def matchClock(clock: Any): Option[D#C] =
    if (clock.getClass == zero.clock.getClass) Some(clock.asInstanceOf[D#C]) else None

  def getRelations(data: D): Set[String]
}

case class ClockInt[C](cur: C, prev: C = 0L)