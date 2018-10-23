package ru.oseval.datahub

/**
  * It supposed that all methods of a datahub are synchronous and have guaranteed effect in subscriber lifetime.
  * E.g. mostly in runtime.
  * @tparam M
  */
trait Datahub[M[_]] {
  def register(facade: EntityFacade): M[Unit]
  def subscribe(entity: Entity,
                subscriber: Entity, --- storage facade instead of entity facade
                lastKnownDataClockOpt: Option[Any]): Boolean
  def unsubscribe(entity: Entity, subscriber: Entity): M[Unit]
  def dataUpdated(entity: Entity)(data: entity.ops.D): M[Unit]
  def syncRelationClocks(entity: Entity, relationClocks: Map[Entity, Any]): Unit
}