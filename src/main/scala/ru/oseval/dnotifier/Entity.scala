package ru.oseval.dnotifier

import ru.oseval.dnotifier.Notifier.{NotifierMessage, NotifyDataUpdated, Register}

import scala.concurrent.Future
import akka.actor.{Actor, ActorRef}
import akka.util.Timeout
import akka.pattern.ask
import ru.oseval.dnotifier.Data.{GetDifferenceFrom, RelatedDataUpdated}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try

trait Entity[D <: Data] {
  // TODO: add type annotation
  val ownId: Any
  val ops: DataOps[D]

  lazy val id: String = ops.makeId(ownId)
}

class LocalDataStorage(createFacade: Entity[_ <: Data] => EntityFacade[_ <: Data],
                       notify: NotifierMessage => Future[Unit],
                       knownData: Map[Entity[_ <: Data], _ <: Data] = Map.empty) {
  private val entities = mutable.Map[String, Entity[_ <: Data]](knownData.keys.map(e => e.id -> e).toSeq: _*)
  private val relations = mutable.Set.empty[String]
  private val datas = mutable.Map[String, _ <: Data](knownData.map { case (e, v) => e.id -> v }.toSeq: _*)

  def addEntity[D <: Data](entity: Entity[D]): Future[Unit] = {
    entities.update(entity.id, entity)
    val Some((e, d)) = isAssignable(entity, datas(entity.id))
    val relationClocks = e.ops.getRelations(d)
      .flatMap(id => datas.get(id).map(d => id -> d.clock)).toMap
    // send current clock to avoid unnecessary update sending (from zero to current)
    notify(Register(createFacade(entity), datas(entity.id).clock, relationClocks))
  }

  def addRelation(entity: Entity[Data]): Unit = {
    entities.update(entity.id, entity)
    relations += entity.id
  }

  def combine(entityId: String, otherData: Data): Future[Unit] = {
    entities.get(entityId).map(combine(_, otherData)) getOrElse Future.unit
  }

  def combine[D <: Data](entity: Entity[D], otherData: D): Future[Unit] = {
    if (relations contains entity.id) {
      val Some((e, d)) = isAssignable(entity, datas(entity.id))
      datas.update(
        entity.id,
        if (datas isDefinedAt entity.id) e.ops.combine(d, otherData) else otherData
      )

      Future.unit
    } else {
      datas.get(entity.id).map { before ⇒
        val Some((e, d)) = isAssignable(entity, datas(entity.id))

        val relatedBefore = entity.ops getRelations before

        datas.update(entity.id, entity.ops.combine(before, otherData))

        val relatedAfter = entity.ops getRelations datas(entity.id)

        relations ++= (entity.ops getRelations otherData)
        relations --= (relatedBefore -- relatedAfter)

        datas --= (relatedBefore -- relatedAfter)
      }.getOrElse {
        datas.update(entity.id, otherData)
      }

      notify(NotifyDataUpdated(entity.id, otherData))
    }
  }

  def diffFromClock(entity: Entity[Data], clock: String): Data =
    entity.ops.diffFromClock(datas.getOrElseUpdate(entity.id, entity.ops.zero), clock)

  def isAssignable[D <: Data](e: Entity[D], data: D): Option[(Entity[D], D)] =
      if (e.ops.zero.getClass isAssignableFrom data.getClass) Option((e, data.asInstanceOf[D]))
      else None

  def get[D <: Data](entity: Entity[D]): Option[D] =
    datas.get(entity.id).flatMap(isAssignable(entity, _))
}

trait NotAssociativeLocalDataStore[D <: NotAssociativeData] extends LocalDataStorage {
//  override def combine(otherData: D): Future[Unit] = {
//    if (data.clock == otherData.previousClock) super.combine(otherData)
//    else
//  }
}

object EntityFacade {
  def unapply[D <: Data](facadeWithData: (EntityFacade[D], D)): Option[(EntityFacade[D], D)] =
    if (facadeWithData._1.entity.ops.zero.getClass isAssignableFrom facadeWithData._2.getClass) Some(facadeWithData)
    else None
}

trait EntityFacade[D <: Data] {
  val entity: Entity[D]

  /**
    * Request explicit data difference from entity
    * @param dataClock
    * @return
    */
  def getUpdatesFrom(dataClock: String)(implicit timeout: Timeout): Future[Data]

  /**
    * Receives updates of related external data
    * @param relatedId
    * @param relatedData
    * @return
    */
  def onUpdate(relatedId: String, relatedData: D)(implicit timeout: Timeout): Future[Unit]
}

case class ActorFacade[D <: Data: ClassTag](entity: Entity[D],
                                            holder: ActorRef) extends EntityFacade[D] {
  override def getUpdatesFrom(dataClock: String)(implicit timeout: Timeout): Future[D] =
    holder.ask(GetDifferenceFrom(entity.id, dataClock)).mapTo[D]

  override def onUpdate (relatedId: String, relatedData: D) (implicit timeout: Timeout): Future[Unit] =
    holder.ask(RelatedDataUpdated(entity.id, relatedId, relatedData)).mapTo[Unit]
}

trait ActorDataMethods { this: Actor =>
  protected val storage: LocalDataStorage

  def handleDataMessage(entity: Entity[Data]): Receive = {
    case GetDifferenceFrom(id, olderClock) if id == entity.id ⇒
      sender() ! storage.diffFromClock(entity, olderClock)

    case RelatedDataUpdated(id, relatedId, relatedUpdate) if id == entity.id ⇒
      storage.combine(relatedId, relatedUpdate)
      sender() ! ()
  }
}