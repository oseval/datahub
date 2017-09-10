package ru.oseval.datahub.data

import ru.oseval.datahub.Entity

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
  * When you want to be sure that your data will reach a destination.
  * Such data is data which sends by partial updates (elements of a list for example)
  * To achieve this guarantee and found gaps it has [[NotAssociativeData.previousId]].
  */
trait AtLeastOnceData extends Data {
  val previousClock: String
}

/**
  * Any data which must apply updates continually (without gaps).
  */
trait NotAssociativeData extends AtLeastOnceData

class SetDataOps[A](makeId: String => String) extends DataOps[SetData[A]] {
  override val ordering = new Ordering[String] {
    override def compare(x: String, y: String) = Ordering.Long.compare(x.toLong, y.toLong)
  }
  override val zero = SetData(System.currentTimeMillis.toString, "0", Seq.empty)

  override def combine(a: SetData[A], b: SetData[A]): SetData[A] = {
    val (first, second) = if (a.clock.toLong > b.clock.toLong) (b, a) else (a, b)
    if (first.clock == second.previousClock) {
      SetData(second.clock, first.previousClock)(
        first.underlying ++ second.underlying,
        first.removed ++ second.removed,
        check first.further and combine them with own or second further
      )
    } else SetData(first.clock, first.previousClock)(first.underlying, first.removed, first.further combine second)

    val total = a.underlying ++ b.underlying
    total
    SetData(a.clock, a.previousClock, b.elements)
  }

  override def diffFromClock(a: SetData[A], from: String) = ???

  override def getRelations(data: SetData[A]): Set[String] = Set.empty

  override def makeId(ownId: Any) = makeId(ownId)
}

case class SetData[+A](clock: String, previousClock: String)
                      (protected[SetDataOps] val underlying: Map[String, A],
                       removed: Map[String, A],
                       further: SetData[A]) extends AtLeastOnceData {
  val elements: Seq[A] = underlying.values.toSeq
  def +[B >: A](el: B): SetData[B] =
    SetData(System.currentTimeMillis.toString, clock, elements :+ el)

  def drop[B >: A](el: B): SetData[B] =
    SetData(System.currentTimeMillis.toString, clock, elements :+ el)
}

trait SetEntity[A](makeId: String => String) extends Entity {
  type D <: SetData[A]
  lazy val ops = new SetDataOps[A](makeId)
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
    * @param data
    * @return
    */
  def getRelations(data: D): Set[String]

  def makeId(ownId: Any): String
}