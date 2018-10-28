package ru.oseval.datahub

import ru.oseval.datahub.data.Data

trait Subscriber {
  /**
    * Receives updates of related external data
    *
    * @param relationId
    * @param relationData
    */
  def onUpdate(relationId: String, relationData: Data): Unit
}

/**
  * It supposed that all methods of a datahub are synchronous and have guaranteed effect in subscriber lifetime.
  * E.g. mostly in runtime.
  * @tparam M
  */
trait Datahub[M[_]] {
  def register(facade: EntityFacade): Unit
  def subscribe(entity: Entity,
                subscriber: Subscriber,
                lastKnownDataClockOpt: Any): Boolean
  def unsubscribe(entity: Entity, subscriber: Subscriber): Unit
  def dataUpdated(entity: Entity)(data: entity.ops.D): Unit
  def syncRelationClocks(entity: Subscriber, relationClocks: Map[Entity, Any]): Unit
}