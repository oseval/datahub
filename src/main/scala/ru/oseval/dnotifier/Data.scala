package ru.oseval.dnotifier

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
trait Data { self ⇒
  val clock: String
}

/**
  * Any data which must apply updates continually (without gaps).
  * To make it associative and found gaps it has [[NotAssociativeData.previousId]].
  */
trait NotAssociativeData extends Data {
  val previousId: String
}

trait DataOps {
  type DInner <: Data
  val ordering: Ordering[String]

  /**
    * Combines two data objects to one
    * @param a
    * @param b
    * @return
    */
  def combine(a: DInner, b: DInner): DInner

  /**
    * Computes diff between `a` and older state with a `from` id
    * @param a
    * @param from
    * @return
    */
  def diffFromClock(a: DInner, from: String): DInner

  /**
    * Zero data.
    */
  val zero: DInner

  /**
    * Returns the entity ids which related to a specified data
    * @param data
    * @return
    */
  def getRelatedEntities(data: DInner): Set[String]
}
