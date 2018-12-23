package ru.oseval.datahub

import ru.oseval.datahub.data.{Data, DataOps}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait Entity {
  val id: String
  val ops: DataOps

  /**
    * Helper to upcast self to Entity
    */
  lazy val lift: Entity = this
}

sealed trait EntityFacade

trait LocalEntityFacade extends EntityFacade {
  val entity: Entity

  /**
    * Request explicit data difference from entity to force data syncing
    * Facade should send data update (from a given clock) to the datahub as reaction on call of this method
    * In other case data will be synced only after next data update
    * @param dataClock
    * @return
    */
  def syncData(dataClock: entity.ops.D#C): Unit
}

trait RemoteEntityFacade extends EntityFacade {
  val ops: DataOps

  /**
    * Request explicit data difference from entity to force data syncing
    * Facade should sync data with remote (from a given clock) as reaction on call of this method
    * In other case data will be synced only after next data update
    * @param dataClock
    * @return
    */
  def syncData(entityId: String, dataClock: ops.D#C): Unit

  def onSubscribe(entity: Entity,
                  subscriber: Subscriber,
                  lastKnownDataClock: Any): Unit

  def onUnsubscribe(entity: Entity, subscriber: Subscriber): Unit
}