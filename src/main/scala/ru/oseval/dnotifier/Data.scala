package ru.oseval.dnotifier

import scala.reflect.ClassTag

object Data {
  sealed trait DataMessage
  case class GetDifferenceFrom(entityId: String, dataClock: String) extends DataMessage
  case class RelatedDataUpdated(toEntityId: String, relatedId: String, data: Data) extends DataMessage

  val timestampOrdering: Ordering[String] = new Ordering[String] {
    private val impl = implicitly[Ordering[Long]]
    override def compare(x: String, y: String): Int = impl.compare(x.toLong, y.toLong)
  }
}

/**
  * Idempotent (due to [[Data.clock]] and commutative (due to [[DataOps.ordering]]) data model.
  */
trait Data {
  val clock: String
}

/**
  * Any data which must apply updates continually (without gaps).
  * To make it associative and found gaps it has [[NotAssociativeData.previousId]].
  */
trait NotAssociativeData extends Data {
  val previousClock: String
  def isCombinableTo[D <: NotAssociativeData](that: D, other: D): Boolean = other.previousClock == that.clock
}

abstract class DataOps[D <: Data] {
  val ordering: Ordering[String]
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
  def diffFromClock(a: D, from: String): D

  /**
    * Returns the entity ids which related to a specified data
    * @param datastorage.addEntity(group)(GroupData(title, Map.empty, Map.empty, System.currentTimeMillis.toString))
    * @return
    */
  def getRelations(data: D): Set[String]

  def makeId(ownId: Any): String
}