package com.github.oseval.datahub

import org.slf4j.Logger
import com.github.oseval.datahub.data.{AtLeastOnceData, ClockInt, Data}

import scala.collection.mutable
import scala.reflect.ClassTag

class LocalDataStorage(log: Logger,
                       createFacade: Entity => LocalEntityFacade,
                       datahub: Datahub, // TODO: WeakReference
                       knownData: Map[Entity, Data] = Map.empty) extends Subscriber {
  private val entities = mutable.Map[String, Entity](knownData.keys.map(e => e.id -> e).toSeq: _*)
  private val relations = mutable.Map.empty[String, mutable.Set[String]] // relation -> entities

  private val pendingSubscriptions = mutable.Set.empty[Entity]  
  private var notSolidRelations = Map.empty[Entity, Any]
  private val datas: mutable.Map[String, Data] = mutable.Map(knownData.map { case (e, v) => e.id -> v }.toSeq: _*)

  private def createFacadeDep(e: Entity) = createFacade(e).asInstanceOf[LocalEntityFacade { val entity: e.type }]

  private def subscribeOnRelation(entity: Entity, relation: Entity) = {
    entities.getOrElseUpdate(relation.id, relation)

    relations.getOrElseUpdate(relation.id, mutable.Set.empty) += entity.id
    val lastKnownData = get(relation) getOrElse {
      datas.update(relation.id, relation.ops.zero)
      relation.ops.zero
    }
    if (!datahub.subscribe(relation, this, lastKnownData.clock))
      pendingSubscriptions += relation
  }
  private def removeRelation(entityId: String, relationId: String): Unit =
    relations.get(relationId).foreach { entityIds =>
      entityIds -= entityId
      if (entityIds.isEmpty) {
        entities.get(relationId).foreach(datahub.unsubscribe(_, this))
        relations -= relationId
        datas -= relationId
      }
    }

  def addEntity(entity: Entity)(_data: entity.ops.D): Unit = {
    entities.update(entity.id, entity)
    val data: entity.ops.D = get(entity).map(entity.ops.combine(_, _data)) getOrElse _data

    datas.update(entity.id, data)

    // send current clock to avoid unnecessary update sending (from zero to current)
    datahub.register(createFacadeDep(entity))

    val (addedRelations, removedRelations) = entity.ops.getRelations(data)
    val entityRelations = addedRelations -- removedRelations
    entityRelations.foreach(subscribeOnRelation(entity, _))
  }

  def combineRelation(entityId: String, otherData: Data): Data =
    entities.get(entityId)
      .map(e =>
        e.ops.matchData(otherData) match {
          case Some(data) => combineRelation(e)(data)
          case None =>
            log.warn("Data {} does not match with entity {}", otherData.getClass.getName, e.getClass.getName: Any)
            otherData
        }
      ).getOrElse {
        log.warn("No data for relation {} found", entityId)
        otherData
      }

  private def combineRelation(entity: Entity)(update: entity.ops.D): entity.ops.D =
    if (relations contains entity.id) {
      val current = get(entity).getOrElse(entity.ops.zero)
      val updated = entity.ops.combine(current, update)

      updated match {
        case data: AtLeastOnceData if !data.isSolid =>
          notSolidRelations = notSolidRelations.updated(
            entity, current.clock
          )

        case _ =>
      }

      datas.update(entity.id, updated)

      updated
    } else {
      log.warn("Entity {} is not registered as relation", entity.id)
      update
    }

  override def onUpdate(relation: Entity)(relationData: relation.ops.D): Unit =
    combineRelation(relation)(relationData)

  private def applyEntityUpdate(entity: Entity)
                               (curData: entity.ops.D,
                                dataUpdate: entity.ops.D,
                                updatedData: entity.ops.D): Unit =
    if (entities isDefinedAt entity.id) {
      val (addedRelations, removedRelations) = entity.ops getRelations dataUpdate

      addedRelations.foreach(subscribeOnRelation(entity, _))
      removedRelations.foreach { rel =>
        removeRelation(entity.id, rel.id)
      }

      datas.update(entity.id, updatedData)

      datahub.dataUpdated(entity)(dataUpdate)
    } else addEntity(entity)(updatedData)

  def combineEntity(entity: Entity)
                   (upd: entity.ops.D#C => entity.ops.D): Unit = {
    val curData = get(entity).getOrElse(entity.ops.zero)
    val dataUpdate = upd(entity.ops.nextClock(curData.clock))
    val updatedData = entity.ops.combine(curData, dataUpdate)

    applyEntityUpdate(entity)(curData, dataUpdate, updatedData)
  }

  def updateEntity(entity: Entity)
                  (upd: ClockInt[entity.ops.D#C] => entity.ops.D => entity.ops.D): Unit = {
    val curData = get(entity).getOrElse(entity.ops.zero)
    val updatedData = upd(ClockInt(entity.ops.nextClock(curData.clock), entity.ops.zero.clock))(curData)
    val dataUpdate = entity.ops.diffFromClock(updatedData, curData.clock)

    applyEntityUpdate(entity)(curData, dataUpdate, updatedData)
  }

  def diffFromClock(entity: Entity)(clock: entity.ops.D#C): entity.ops.D =
    entity.ops.diffFromClock({
      get(entity).getOrElse {
        datas.update(entity.id, entity.ops.zero)
        entity.ops.zero
      }
    }, clock)

  def get[D <: Data](entityId: String)(implicit tag: ClassTag[D]): Option[D] =
    datas.get(entityId).flatMap(d =>
      if (tag.runtimeClass.isAssignableFrom(d.getClass)) Some(tag.runtimeClass.cast(d).asInstanceOf[D]) else None
    )

  def get[D <: Data](entity: Entity): Option[entity.ops.D] =
    datas.get(entity.id).map(d =>
      entity.ops.matchData(d) match {
        case Some(data) => data
        case None =>
          throw new Exception(
            "Inconsistent state of local storage: entity " + entity.id + " does not match to data " + d.getClass.getName
          )
      }
    )

  /**
    * Calls of this method must be scheduled with an interval
    * @return
    */
  def checkDataIntegrity: Boolean = {
    pendingSubscriptions.foreach { relation =>
      val relClock = datas.get(relation.id).map(_.clock).getOrElse(relation.ops.zero.clock)
      if (datahub.subscribe(relation, this, relClock)) pendingSubscriptions -= relation
    }

    if (notSolidRelations.nonEmpty) datahub.syncRelationClocks(this, notSolidRelations)

    notSolidRelations.isEmpty
  }
}
