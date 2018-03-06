package ru.oseval.datahub

import ru.oseval.datahub.data.Data

trait Datahub[M[_]] {
  def register(facade: EntityFacade)
              (lastClock: facade.entity.ops.D#C,
               relationClocks: Map[Entity, Any],
               forcedSubscribers: Set[Entity]): M[Unit]
  def dataUpdated(entityId: String, // TODO: may be EntityFacade instead?
                  _data: Data,
                  addedRelations: Set[Entity],
                  removedRelations: Set[Entity],
                  forcedSubscribers: Set[Entity]): M[Unit]
  def syncRelationClocks(entityId: String, relationClocks: Map[Entity, Any]): M[Unit]
}