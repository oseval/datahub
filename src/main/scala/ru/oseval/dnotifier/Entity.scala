package ru.oseval.dnotifier

import ru.oseval.dnotifier.ActorNotifier.{NotifierMessage, NotifyDataUpdated, Register}

import scala.concurrent.Future
import akka.actor.{Actor, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout
import akka.pattern.ask
import ru.oseval.dnotifier.Data.{GetDifferenceFrom, RelatedDataUpdated}

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

trait Entity {
  type D <: Data
  // TODO: add type annotation
  val ownId: Any
  val ops: DataOps[D]

  lazy val id: String = ops.makeId(ownId)

  def matchData(data: Data): Option[D] =
    if (ops.zero.getClass == data.getClass)
      Option(data.asInstanceOf[D])
    else
      None
}

class LocalDataStorage(log: LoggingAdapter,
                       createFacade: Entity => EntityFacade,
                       notify: NotifierMessage => Future[Unit],
                       knownData: Map[Entity, Data] = Map.empty) {
  private val entities = mutable.Map[String, Entity](knownData.keys.map(e => e.id -> e).toSeq: _*)
  private val relations = mutable.Set.empty[String]
  private val datas = mutable.Map(knownData.map { case (e, v) => e.id -> v }.toSeq: _*)

  def addEntity(entity: Entity)(_data: entity.D): Future[Unit] = {
    entities.update(entity.id, entity)
    val data = get(entity).getOrElse {
      datas.update(entity.id, _data)
      _data
    }

    val relationClocks = entity.ops.getRelations(data)
      .flatMap(id => datas.get(id).map(d => id -> d.clock)).toMap
    // send current clock to avoid unnecessary update sending (from zero to current)
    notify(Register(createFacade(entity), data.clock, relationClocks))
  }

  def addRelation(entity: Entity): Unit = {
    entities.update(entity.id, entity)
    relations += entity.id
  }

  def combine(entityId: String, otherData: Data): Future[Unit] =
    entities.get(entityId)
      .map(e =>
        e.matchData(otherData) match {
          case Some(data) => combine(e)(data)
          case None =>
            Future.failed(new Exception(
              "Data " + otherData.getClass.getName + " does not match with entity " + e.getClass.getName
            ))
        }
      )
      .getOrElse(Future.unit)

  def combine(entity: Entity)(otherData: entity.D): Future[Unit] =
    if (relations contains entity.id) {
      val result =
        get(entity)
          .map(entity.ops.combine(_, otherData))
          .getOrElse(otherData)

      datas.update(entity.id, result)

      Future.unit
    } else {
      get(entity).map { before ⇒
        val relatedBefore = entity.ops getRelations before

        val after = entity.ops.combine(before, otherData)
        datas.update(entity.id, after)

        val relatedAfter = entity.ops getRelations after

        relations ++= (entity.ops getRelations otherData)
        relations --= (relatedBefore -- relatedAfter)

        datas --= (relatedBefore -- relatedAfter)
      }.getOrElse {
        datas.update(entity.id, otherData)
      }

      notify(NotifyDataUpdated(entity.id, otherData))
    }

  def diffFromClock(entity: Entity, clock: String): entity.D =
    entity.ops.diffFromClock({
      get(entity).getOrElse {
        datas.update(entity.id, entity.ops.zero)
        entity.ops.zero
      }
    }, clock)

  def get[D <: Data](entity: Entity): Option[entity.D] =
    datas.get(entity.id).map(d =>
      entity.matchData(d) match {
        case Some(data) => data
        case None =>
          throw new Exception(
            "Inconsistent state of localstorage: entity " + entity.id + " does not match to data " + d.getClass.getName
          )
      }
    )
}

trait NotAssociativeLocalDataStore[D <: NotAssociativeData] extends LocalDataStorage {
//  override def combine(otherData: D): Future[Unit] = {
//    if (data.clock == otherData.previousClock) super.combine(otherData)
//    else
//  }
}

trait EntityFacade {
  val entity: Entity

  /**
    * Request explicit data difference from entity
    * @param dataClock
    * @return
    */
  def getUpdatesFrom(dataClock: String)(implicit timeout: FiniteDuration): Future[entity.D]

  /**
    * Receives updates of related external data
    * @param relatedId
    * @param relatedData
    * @return
    */
  def onUpdate(relatedId: String, relatedData: Data)(implicit timeout: FiniteDuration): Future[Unit]
}

case class ActorFacade(entity: Entity, holder: ActorRef) extends EntityFacade {
  override def getUpdatesFrom(dataClock: String)(implicit timeout: FiniteDuration): Future[entity.D] =
    holder.ask(GetDifferenceFrom(entity.id, dataClock))(timeout).asInstanceOf[Future[entity.D]]

  override def onUpdate(relatedId: String, relatedData: Data)(implicit timeout: FiniteDuration): Future[Unit] =
    holder.ask(RelatedDataUpdated(entity.id, relatedId, relatedData))(timeout).mapTo[Unit]
}

trait ActorDataMethods { this: Actor =>
  protected val storage: LocalDataStorage

  def handleDataMessage(entity: Entity): Receive = {
    case GetDifferenceFrom(id, olderClock) if id == entity.id =>
      sender() ! storage.diffFromClock(entity, olderClock)

    case RelatedDataUpdated(id, relatedId, relatedUpdate) if id == entity.id =>
      storage.combine(relatedId, relatedUpdate)
      sender() ! ()
  }
}