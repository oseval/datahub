package com.github.oseval.datahub

import com.github.oseval.datahub.data.Data

trait Subscriber {
  /**
    * Receives updates of related external data
    *
    * @param relation
    * @param relationData
    */
  def onUpdate(relation: Entity)(relationData: relation.ops.D): Unit
}

/**
  * It supposed that all methods of a datahub are synchronous and have guaranteed effect in subscriber lifetime.
  * E.g. mostly in runtime.
  * @tparam M
  */
trait Datahub {
  def register(facade: EntityFacade): Unit
  def subscribe(entity: Entity,
                subscriber: Subscriber,
                lastKnownDataClock: Any): Boolean
  def unsubscribe(entity: Entity, subscriber: Subscriber): Unit
  def dataUpdated(entity: Entity)(data: entity.ops.D): Unit
  def syncRelationClocks(subscriber: Subscriber, relationClocks: Map[Entity, Any]): Unit
}
