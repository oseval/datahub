package ru.oseval.datahub

import scala.concurrent.Future
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