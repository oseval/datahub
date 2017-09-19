package ru.oseval.datahub.data

object Data {
  sealed trait DataMessage
  case class GetDifferenceFrom(entityId: String, dataClock: String) extends DataMessage
  case class RelatedDataUpdated(toEntityId: String, relatedId: String, data: Data) extends DataMessage
}

/**
  * Idempotent (due to [[Data.clock]] and commutative (due to [[DataOps.ordering]]) data model.
  */
trait Data {
  type C
  val clock: C
}

/**
  * When you want to be sure that your data will reach a destination.
  * Such data is data which sends by partial updates (elements of a list for example)
  * To achieve this guarantee and found gaps it has [[NotAssociativeData.previousId]].
  */
trait AtLeastOnceData extends Data {
  val previousClock: C
}

/**
  * Any data which must apply updates continually (without gaps).
  */
trait NotAssociativeData extends AtLeastOnceData

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

  /**
    * Returns the entity ids which related to a specified data
    * @param data
    * @return
    */
  def getRelations(data: D): Set[String]

  def matchData(data: Data): Option[D] =
    if (zero.getClass == data.getClass)
      Option(data.asInstanceOf[D])
    else
      None

  def matchClock(clock: Any): Option[D#C] =
    if (clock.getClass == zero.clock.getClass) Some(clock.asInstanceOf[D#C]) else None
}