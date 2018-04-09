package ru.oseval.datahub.data

import ru.oseval.datahub.{Entity, EntityFacade}

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

/**
  * If your data is compound from some other datas then you can use this trait to automate data flow
  */
trait CompoundData extends AtLeastOnceData {
  protected val children: Set[Data]
  override lazy val isSolid: Boolean = children.forall { case d: AtLeastOnceData => d.isSolid case _ => true }
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

  // TODO: store as SetData?
  def getRelations(data: D): (Set[Entity], Set[Entity])

  def getForcedSubscribers(data: D): Set[EntityFacade]

  def approveRelation(data: D, relationId: String): Boolean = true

  // TODO: Entity Ops
  def createFacadeFromEntityId(entityId: String): Option[EntityFacade] = None
}

case class ClockInt[C](cur: C, prev: C)



//       ai     alo    eff
//
//a                    +      prevId
//
//c      +      +      +      order
//
//i      +      +      +      id


