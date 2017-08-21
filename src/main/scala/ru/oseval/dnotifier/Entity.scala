package ru.oseval.dnotifier

import ru.oseval.dnotifier.Notifier.{NotifierMessage, NotifyDataUpdated, Register}

import scala.concurrent.Future
import akka.actor.{Actor, ActorRef}
import akka.util.Timeout
import akka.pattern.ask
import ru.oseval.dnotifier.Data.{GetDifferenceFrom, RelatedDataUpdated}

import scala.collection.mutable
import scala.reflect.ClassTag

trait Entity[D <: Data] {
  // TODO: add type annotation
  val ownId: Any
  val ops: DataOps[D]

  lazy val id: String = ops.makeId(ownId)

  def matchData(data: Data): Option[D] =
    if (ops.zero.getClass isAssignableFrom data.getClass)
      Option(data.asInstanceOf[D])
    else
      None
}

class LocalDataStorage(createFacade: Entity[_ <: Data] => EntityFacade[_ <: Data],
                       notify: NotifierMessage => Future[Unit],
                       knownData: Map[Entity[_ <: Data], _ <: Data] = Map.empty) {
  private val entities = mutable.Map[String, Entity[_ <: Data]](knownData.keys.map(e => e.id -> e).toSeq: _*)
  private val relations = mutable.Set.empty[String]
  private val datas = mutable.Map(knownData.map { case (e, v) => e.id -> v }.toSeq: _*)

  def addEntity[D <: Data](entity: Entity[D]): Future[Unit] = {
    entities.update(entity.id, entity)
    entity.matchData(datas(entity.id)).map { data =>
      val relationClocks = entity.ops.getRelations(data)
        .flatMap(id => datas.get(id).map(d => id -> d.clock)).toMap
      // send current clock to avoid unnecessary update sending (from zero to current)
      notify(Register(createFacade(entity), data.clock, relationClocks))
    }.get
  }

  def addRelation(entity: Entity[Data]): Unit = {
    entities.update(entity.id, entity)
    relations += entity.id
  }

  def combine[D <: Data](entityId: String, otherData: D): Future[Unit] =
    entities.get(entityId)
      .map(e => combine(e.asInstanceOf[Entity[Data]], otherData))
      .getOrElse(Future.unit)

  def combine[D <: Data](entity: Entity[D], otherData: D): Future[Unit] =
    if (relations contains entity.id) {
      val result =
        datas.get(entity.id).flatMap(entity.matchData).map(data =>
          entity.ops.combine(data, otherData)
        ) getOrElse otherData

      datas.update(entity.id, result.asInstanceOf[Data])

      Future.unit
    } else {
      datas.get(entity.id).flatMap(entity.matchData).map { before ⇒
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

  def diffFromClock(entity: Entity[Data], clock: String): Data =
    entity.ops.diffFromClock(datas.getOrElseUpdate(entity.id, entity.ops.zero), clock)

  object withMatchedOps {
    def apply[D <: Data](ed: (Entity[D], _ <: Data)): Option[(Entity[D], D)] =
      if (ed._1.ops.zero.getClass isAssignableFrom ed._2.getClass)
        Option(ed._1, ed._2.asInstanceOf[D])
      else
        None
  }

  def get[D <: Data](entity: Entity[D]): Option[D] =
    datas.get(entity.id).flatMap(entity.matchData)
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