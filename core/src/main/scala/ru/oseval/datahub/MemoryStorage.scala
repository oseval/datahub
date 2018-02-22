package ru.oseval.datahub

import java.util.concurrent.ConcurrentHashMap

import ru.oseval.datahub.AsyncDatahub.Storage

import scala.concurrent.Future

class MemoryStorage extends Storage {
  private val ids: ConcurrentHashMap[String, Any] = new ConcurrentHashMap[String, Any]

  override def increase(entity: Entity)(dataClock: entity.ops.D#C): Future[Unit] = {
    val res = ids.computeIfPresent(entity.id, { case (k: String, _clk: Any) =>
      entity.ops.matchClock(_clk).filter(entity.ops.ordering.gt(_, dataClock)) getOrElse dataClock
    })

    if (res == null) ids.putIfAbsent(entity.id, dataClock)

    Future.unit
  }

  override def getLastClock(entity: Entity): Future[Option[entity.ops.D#C]] =
    Future.successful(Option(ids.get(entity.id)).flatMap(entity.ops.matchClock(_)))
}
