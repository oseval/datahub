package com.github.oseval.datahub.remote

import java.util.concurrent.atomic.AtomicReference

import com.github.oseval.datahub.remote.RemoteFacade.SubscriptionsManagement
import com.github.oseval.datahub.{Datahub, Entity, RemoteEntityFacade, Subscriber}
import com.github.oseval.datahub.remote.RemoteSubscriber.SubsOps

/**
  * This is one of the remotes interactions parts
  *
  * LocalSubscriber -> Datahub -> RemoteFacade - - -> RemoteSubscriber -> Datahub -> LocalFacade
  */
object RemoteFacade {
  trait SubscriptionsManagement { this: RemoteEntityFacade =>
    /**
      * Storage to save data in case of local datahub crashed
      */
    val subscriptionStorage: SubscriptionStorage

    /**
      * Current subscriptions
      */
    val subscriptions = new AtomicReference(SubsOps.zero)

    /**
      * Calls by facade when subscriptions are updated
      * @param entity
      * @param data
      */
    protected def updateSubscriptions(update: SubsOps.D): Unit

    def syncSubscriptions(clock: Long): Unit =
      updateSubscriptions(SubsOps.diffFromClock(subscriptions.get(), clock))

    override def onSubscribe(entity: Entity,
                             subscriber: Subscriber,
                             lastKnownDataClock: Any
                            ): Unit =
      subscriptions.accumulateAndGet(SubsOps.zero,  { (_, curData: SubsOps.D) =>
        val newClock = System.currentTimeMillis
        val updatedSubs = curData.data.subs.add(entity -> lastKnownDataClock, newClock)
        val newData = curData.updated(curData.data.copy(subs = updatedSubs, clock = newClock))

        val diff = SubsOps.diffFromClock(newData, curData.clock)
        updateSubscriptions(diff)
        subscriptionStorage.onUpdate(newData.data)

        newData
      })

    override def onUnsubscribe(entity: Entity, subscriber: Subscriber): Unit = {
      subscriptions.accumulateAndGet(SubsOps.zero,  { (_, curData: SubsOps.D) =>
        curData.data.elemMap.get(entity).map { relationClk =>
          val newClock = System.currentTimeMillis
          val updatedSubs = curData.data.subs.remove(entity -> relationClk, newClock)
          val newData = curData.updated(curData.data.copy(subs = updatedSubs, clock = newClock))

          val diff = SubsOps.diffFromClock(newData, curData.clock)
          updateSubscriptions(diff)
          subscriptionStorage.onUpdate(newData.data)

          newData
        }.getOrElse(curData)
      })
    }
  }
}

/**
  * This is one of the remotes interactions parts
  *
  * LocalSubscriber -> Datahub -> RemoteFacade - - -> RemoteSubscriber -> Datahub -> LocalFacade
  */
trait SimpleRemoteFacade extends RemoteEntityFacade with SubscriptionsManagement {
  // TODO: Must it be the SoftReference?
  protected val datahub: Datahub

  override def syncData(entityId: String, dataClock: ops.D#C): Unit

  /**
    * Calls by external connection when data update comes from remote
    * @param entity
    * @param data
    */
  def onUpdate(entity: Entity)(data: entity.ops.D): Unit = datahub.dataUpdated(entity)(data)
}
