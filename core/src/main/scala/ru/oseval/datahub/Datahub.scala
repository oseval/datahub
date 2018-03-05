package ru.oseval.datahub

import ru.oseval.datahub.data.Data

trait Datahub[M[_]] {
  def register(facade: EntityFacade)
              (lastClock: facade.entity.ops.D#C,
               relationClocks: Map[Entity, Any],
               forcedSubscribers: Set[String]): M[Unit]
  def dataUpdated(entityId: String, // TODO: may be EntityFacade instead?
                  _data: Data,
                  forcedSubscribers: Set[String]): M[Unit]
  def syncRelationClocks(entityId: String, relationClocks: Map[Entity, Any]): M[Unit]
}