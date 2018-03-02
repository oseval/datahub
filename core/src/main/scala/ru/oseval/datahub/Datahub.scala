package ru.oseval.datahub

import ru.oseval.datahub.data.Data

trait Datahub[M[_]] {
  def register(facade: EntityFacade)(lastClock: facade.entity.ops.D#C, relationClocks: Map[String, Any]): M[Unit]
  def dataUpdated(entityId: String, _data: Data): M[Unit]
  def syncRelationClocks(entityId: String, relationClocks: Map[String, Any]): M[Unit]
}