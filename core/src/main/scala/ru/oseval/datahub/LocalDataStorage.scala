package ru.oseval.datahub

import org.slf4j.Logger
import ru.oseval.datahub.data.{AtLeastOnceData, ClockInt, Data}

import scala.collection.mutable
import scala.reflect.ClassTag

class LocalDataStorage[M[_]](log: Logger,
                             createFacade: Entity => EntityFacade,
                             datahub: Datahub[M],
                             knownData: Map[Entity, Data] = Map.empty) {
  private val entities = mutable.Map[String, Entity](knownData.keys.map(e => e.id -> e).toSeq: _*)
  private val relations = mutable.Map.empty[String, mutable.Set[String]] // relation -> entities

  private val pendingSubscriptions = mutable.Set.empty[Entity]  
  private var notSolidRelations = Map.empty[Entity, Any]
  private val datas = mutable.Map(knownData.map { case (e, v) => e.id -> v }.toSeq: _*)

  private def createFacadeDep(e: Entity) = createFacade(e).asInstanceOf[EntityFacade { val entity: e.type }]

  private def addRelation(entityId: String, relationId: String): Unit =
    relations.getOrElseUpdate(relationId, mutable.Set.empty) += entityId
  private def subscribeOnRelation(entity: Entity, relation: Entity, lastKnownClock: Any) = {
    addRelation(entity.id, relation.id)
    if (!datahub.subscribe(relation, entity, Some(lastKnownClock)))
      pendingSubscriptions += relation
  }
  private def removeRelation(entityId: String, relationId: String): Unit =
    relations.get(relationId).foreach { entityIds =>
      entityIds -= entityId
      if (entityIds.isEmpty) {
        relations -= relationId
        datas -= relationId
      }
    }

  def addEntity(entity: Entity)(_data: entity.ops.D): Unit = {
    entities.update(entity.id, entity)
    val data: entity.ops.D = get(entity).getOrElse {
      datas.update(entity.id, _data)
      _data
    }

    // send current clock to avoid unnecessary update sending (from zero to current)
    datahub.register(createFacadeDep(entity))

    val (addedRelations, removedRelations) = entity.ops.getRelations(data)
    val entityRelations = addedRelations -- removedRelations
    entityRelations.foreach { relation =>
      val clock = datas.get(relation.id).map(_.clock) getOrElse relation.ops.zero.clock
      subscribeOnRelation(entity, relation, clock)
    }
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

  private def applyEntityUpdate(entity: Entity)
                               (curData: entity.ops.D,
                                dataUpdate: entity.ops.D,
                                updatedData: entity.ops.D): M[Unit] =
    if (entities isDefinedAt entity.id) {
      val (addedRelations, removedRelations) = entity.ops getRelations dataUpdate

      addedRelations.foreach { rel =>
        val relClock = datas.get(rel.id).map(_.clock).getOrElse(rel.ops.zero.clock)
        subscribeOnRelation(entity, rel, relClock)
      }
      removedRelations.foreach { rel =>
        removeRelation(entity.id, rel.id)
        datahub.unsubscribe(_, entity)
      }

      datas.update(entity.id, updatedData)

      datahub.dataUpdated(entity)(dataUpdate)
    } else addEntity(entity)(updatedData)

  def combineEntity(entity: Entity)
                   (upd: entity.ops.D#C => entity.ops.D): M[Unit] = {
    val curData = get(entity).getOrElse(entity.ops.zero)
    val dataUpdate = upd(entity.ops.nextClock(curData.clock))
    val updatedData = entity.ops.combine(curData, dataUpdate)

    applyEntityUpdate(entity)(curData, dataUpdate, updatedData)
  }

  def updateEntity(entity: Entity)
                  (upd: ClockInt[entity.ops.D#C] => entity.ops.D => entity.ops.D): M[Unit] = {
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
    pendingSubscriptions = pendingSubscriptions.filter { relation =>
      val relClock = datas.get(relation.id).map(_.clock).getOrElse(relation.ops.zero.clock)
      datahub.subscribe(relation, )
    }
    if (notSolidRelations.nonEmpty) (entities -- relations.keySet).headOption.foreach { case (_, e) =>
      datahub.syncRelationClocks(e, notSolidRelations)
    }

    notSolidRelations.isEmpty
  }
}
