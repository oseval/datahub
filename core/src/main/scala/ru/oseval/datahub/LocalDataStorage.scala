package ru.oseval.datahub

import org.slf4j.Logger
import ru.oseval.datahub.Datahub.{DataUpdated, DatahubMessage, Register}
import ru.oseval.datahub.data.{ClockInt, Data}

import scala.collection.mutable
import scala.concurrent.Future
import scala.reflect.ClassTag

class LocalDataStorage(log: Logger,
                       createFacade: Entity => EntityFacade,
                       notify: DatahubMessage => Future[Unit],
                       knownData: Map[Entity, Data] = Map.empty) {
  private val entities = mutable.Map[String, Entity](knownData.keys.map(e => e.id -> e).toSeq: _*)
  private val relations = mutable.Set.empty[String]
  private val datas = mutable.Map(knownData.map { case (e, v) => e.id -> v }.toSeq: _*)

  private def createFacadeDep(e: Entity) = createFacade(e).asInstanceOf[EntityFacade { val entity: e.type }]

  def addEntity(entity: Entity)(_data: entity.ops.D): Future[Unit] = {
    entities.update(entity.id, entity)
    val data: entity.ops.D = get(entity).getOrElse {
      datas.update(entity.id, _data)
      _data
    }

    val relationClocks = entity.ops.getRelations(data)
      .flatMap(id => datas.get(id).map(d => id -> d.clock)).toMap
    // send current clock to avoid unnecessary update sending (from zero to current)
    notify(Register(createFacadeDep(entity), relationClocks)(data.clock))
  }

  def addRelation(entity: Entity): Unit = {
    entities.update(entity.id, entity)
    relations += entity.id
  }

  def combine(entityId: String, otherData: Data): Future[Unit] =
    entities.get(entityId)
      .map(e =>
        e.ops.matchData(otherData) match {
          case Some(data) => combineRelation(e)(data)
          case None =>
            Future.failed(new Exception(
              "Data " + otherData.getClass.getName + " does not match with entity " + e.getClass.getName
            ))
        }
      )
      .getOrElse(Future.unit)

  private def combineRelation(entity: Entity)(update: entity.ops.D): Future[Unit] = {
    if (relations contains entity.id) {
      val result =
        get(entity)
          .map(entity.ops.combine(_, update))
          .getOrElse(update)

      // TODO: check if data is solid
      datas.update(entity.id, result)
    } else {
      log.warn("No data for relation {} found", entity.id)
    }

    Future.unit
  }

  private def applyEntityUpdate(entity: Entity)
                               (curData: entity.ops.D,
                                dataUpdate: entity.ops.D,
                                updatedData: entity.ops.D) =
    if (entities isDefinedAt entity.id) {
      val relatedBefore = entity.ops getRelations curData

      val relatedAfter = entity.ops getRelations updatedData

      relations ++= relatedAfter
      relations --= (relatedBefore -- relatedAfter)
      datas --= (relatedBefore -- relatedAfter)

      datas.update(entity.id, updatedData)

      notify(DataUpdated(entity.id, dataUpdate))
    } else addEntity(entity)(updatedData)

  def combineEntity(entity: Entity)
                   (upd: entity.ops.D#C => entity.ops.D): Future[Unit] = {
    val curData = get(entity).getOrElse(entity.ops.zero)
    val dataUpdate = upd(entity.ops.nextClock(curData.clock))
    val updatedData = entity.ops.combine(curData, dataUpdate)

    applyEntityUpdate(entity)(curData, dataUpdate, updatedData)
  }

  def updateEntity(entity: Entity)
                  (upd: ClockInt[entity.ops.D#C] => entity.ops.D => entity.ops.D): Future[Unit] = {
    val curData = get(entity).getOrElse(entity.ops.zero)
    val updatedData = upd(ClockInt(entity.ops.nextClock(curData.clock), curData.clock))(curData)
    val dataUpdate = entity.ops.diffFromClock(updatedData, curData.clock)

    applyEntityUpdate(entity)(curData, dataUpdate, updatedData)
  }

  def diffFromUnknownClock(entity: Entity, clock: Any): entity.ops.D =
    diffFromClock(entity)(entity.ops.matchClock(clock) getOrElse entity.ops.zero.clock)

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
}
