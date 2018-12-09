package ru.oseval.datahub

import ru.oseval.datahub.data.SetData
import ru.oseval.datahub.utils.Commons.Id

import scala.collection.SortedMap
import scala.collection.concurrent.TrieMap

//trait EntityIdsStore[M[_]] {
//  def loadEntityIds(): M[SetData[Entity, Long]]
//  def addEntityId(entityId: String): Unit
//  def removeEntityId(entityId: String): Unit
//}
//
//class InMemoryEntityIdsStore extends EntityIdsStore[Id] {
//  private val entityIds: TrieMap[String, Boolean] = TrieMap.empty[String, Boolean]
//
//  override def loadEntityIds(): Id[SetData[String, Long]] = SetData(SortedMap(entityIds.readOnlySnapshot().map(0L -> _._1).toSeq: _*), SortedMap.empty[Long])
//
//  override def addEntityId(entityId: String): Unit = entityIds.update(entityId, true)
//
//  override def removeEntityId(entityId: String): Unit = entityIds -= entityId
//}