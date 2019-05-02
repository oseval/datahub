package com.github.oseval.datahub

import org.slf4j.Logger
import com.github.oseval.datahub.data.{AtLeastOnceData, ClockInt, Data, OpsWithRelations}

import scala.collection.mutable
import scala.ref.WeakReference
import scala.reflect.ClassTag

trait EntityWithRelations {
  def relations: (Set[Entity], Set[Entity])
}

class RelationsManager(weakDatahub: WeakReference[Datahub],
                       log: Logger,
                       onRelationUpdate: (Entity, Data) => Unit,
                       knownData: Map[Entity, Data] = Map.empty) extends Subscriber {
  protected type EntityId = String
  protected type RelationId = EntityId

  def datahub: Datahub = weakDatahub()

  protected val entities = mutable.Map[EntityId, Entity](knownData.keys.map(e => e.id -> e).toSeq: _*)
  protected val datas: mutable.Map[EntityId, Data] = mutable.Map(knownData.map { case (e, v) => e.id -> v }.toSeq: _*)

  private val pendingSubscriptions = mutable.Set.empty[Entity]
  private var notSolidRelations = Map.empty[Entity, Any]

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

  def subscribeOnRelation(relation: Entity): Unit = {
    entities.getOrElseUpdate(relation.id, relation)

    val lastKnownData = get(relation) getOrElse {
      datas.update(relation.id, relation.ops.zero)
      relation.ops.zero
    }

    if (!datahub.subscribe(relation, this, lastKnownData.clock))
      pendingSubscriptions += relation
  }

  def removeRelation(relationId: String): Unit = {
    entities.get(relationId).foreach(datahub.unsubscribe(_, this))
    datas -= relationId
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

  protected def combineRelation(entity: Entity)(update: entity.ops.D): entity.ops.D = {
    val current = get(entity).getOrElse(entity.ops.zero)
    val updated = entity.ops.merge(current, update)

    onRelationUpdate(entity, update)

    updated match {
      case data: AtLeastOnceData if !data.isSolid =>
        notSolidRelations = notSolidRelations.updated(
          entity, current.clock
        )

      case _ =>
    }

    datas.update(entity.id, updated)

    updated
  }

  override def onUpdate(relation: Entity)(relationData: relation.ops.D): Unit =
    combineRelation(relation)(relationData)

  override def onUpdate(relationId: String, relationData: Data): Unit =
    combineRelation(relationId, relationData)

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

class LocalDataStorage(weakDatahub: WeakReference[Datahub],
                       createSource: Entity => LocalDatasource,
                       log: Logger,
                       onRelationUpdate: (Entity, Data) => Unit,
                       knownData: Map[Entity, Data] = Map.empty)
  extends RelationsManager(weakDatahub, log, onRelationUpdate, knownData) {

  private val relations = mutable.Map.empty[RelationId, mutable.Set[EntityId]]

  private def createSourceDep(e: Entity) =
    createSource(e).asInstanceOf[LocalDatasource { val entity: e.type }]

  private def subscribeOnRelation(entity: Entity, relation: Entity): Unit = {
    relations.getOrElseUpdate(relation.id, mutable.Set.empty) += entity.id
    subscribeOnRelation(relation)
  }

  private def removeRelation(entityId: String, relationId: String): Unit =
    relations.get(relationId).foreach { entityIds =>
      entityIds -= entityId
      if (entityIds.isEmpty) {
        removeRelation(relationId)

        relations -= relationId
      }
    }

  def addEntity(entity: Entity)(_data: entity.ops.D): Unit = {
    entities.update(entity.id, entity)
    val data: entity.ops.D = get(entity).map(entity.ops.merge(_, _data)) getOrElse _data

    datas.update(entity.id, data)

    // send current clock to avoid unnecessary update sending (from zero to current)
    datahub.register(createSourceDep(entity))

    entity.ops match {
      case ops: OpsWithRelations[entity.ops.D @unchecked] =>
        val (addedRelations, removedRelations) = ops.getRelations(data)
        val entityRelations = addedRelations -- removedRelations
        entityRelations.foreach(subscribeOnRelation(entity, _))
      case _ =>
    }

  }

  override protected def combineRelation(entity: Entity)(update: entity.ops.D): entity.ops.D =
    if (relations contains entity.id)
      super.combineRelation(entity)(update)
    else {
      log.warn("Entity {} is not registered as relation", entity.id)
      update
    }

  private def applyEntityUpdate(entity: Entity)
                               (curData: entity.ops.D,
                                dataUpdate: entity.ops.D,
                                updatedData: entity.ops.D): Unit =
    if (entities isDefinedAt entity.id) {
      entity.ops match {
        case ops: OpsWithRelations[entity.ops.D @unchecked] =>
          val (addedRelations, removedRelations) = ops getRelations dataUpdate

          addedRelations.foreach(subscribeOnRelation(entity, _))
          removedRelations.foreach { rel =>
            removeRelation(entity.id, rel.id)
          }

        case _ =>
      }

      datas.update(entity.id, updatedData)

      datahub.dataUpdated(entity)(dataUpdate)
    } else addEntity(entity)(updatedData)

  def combineEntity(entity: Entity)
                   (upd: entity.ops.D#C => entity.ops.D): Unit = {
    val curData = get(entity).getOrElse(entity.ops.zero)
    val dataUpdate = upd(entity.ops.nextClock(curData.clock))
    val updatedData = entity.ops.merge(curData, dataUpdate)

    applyEntityUpdate(entity)(curData, dataUpdate, updatedData)
  }

  def updateEntity(entity: Entity)
                  (upd: ClockInt[entity.ops.D#C] => entity.ops.D => entity.ops.D): Unit = {
    val curData = get(entity).getOrElse(entity.ops.zero)
    val updatedData = upd(ClockInt(entity.ops.nextClock(curData.clock), entity.ops.zero.clock))(curData)
    val dataUpdate = entity.ops.diffFromClock(updatedData, curData.clock)

    applyEntityUpdate(entity)(curData, dataUpdate, updatedData)
  }

  def diffFromClock(entityId: String, clock: Any): Option[(Entity, Data)] =
    entities.get(entityId).map { e =>
      val clk: e.ops.D#C = e.ops.matchClock(clock) getOrElse e.ops.zero.clock
      e -> diffFromClock(e)(clk)
    }

  def diffFromClock(entity: Entity)(clock: entity.ops.D#C): entity.ops.D =
    entity.ops.diffFromClock({
      get(entity).getOrElse {
        datas.update(entity.id, entity.ops.zero)
        entity.ops.zero
      }
    }, clock)
}
