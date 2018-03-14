package ru.oseval.datahub

trait Datahub[M[_]] {
  def register(facade: EntityFacade)
              (lastClock: facade.entity.ops.D#C,
               relationClocks: Map[Entity, Any],
               forcedSubscribers: Set[EntityFacade]): M[Unit]
  def subscribe(entity: Entity, // this must be entity to get ops and compare clocks
                subscriber: Entity,
                lastKnownDataClockOpt: Option[Any]): Unit
  def unsubscribe(entity: Entity, subscriber: Entity): Unit
  def dataUpdated(entity: Entity, forcedSubscribers: Set[EntityFacade])(data: entity.ops.D): M[Unit]
  def syncRelationClocks(entity: Entity, relationClocks: Map[Entity, Any]): M[Unit]
}