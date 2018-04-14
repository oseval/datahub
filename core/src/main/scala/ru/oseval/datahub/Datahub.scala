package ru.oseval.datahub

trait Datahub[M[_]] {
  def register(facade: EntityFacade)
              (lastClock: facade.entity.ops.D#C,
               relationClocks: Map[Entity, Any],
               forcedSubscribers: Set[EntityFacade]): M[Unit]
  def subscribe(entity: Entity,
                subscriber: Entity,
                lastKnownDataClockOpt: Option[Any]): M[Unit]
  def unsubscribe(entity: Entity, subscriber: Entity): M[Unit]
  def dataUpdated(entity: Entity, forcedSubscribers: Set[EntityFacade])(data: entity.ops.D): M[Unit]
  def syncRelationClocks(entity: Entity, relationClocks: Map[Entity, Any]): M[Unit]
}