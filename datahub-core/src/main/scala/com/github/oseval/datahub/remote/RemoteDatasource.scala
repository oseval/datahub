package com.github.oseval.datahub.remote

import java.util.concurrent.atomic.AtomicReference

import com.github.oseval.datahub.data.Data
import com.github.oseval.datahub.remote.RemoteDatasourceConnector.SubscriptionsManagement
import com.github.oseval.datahub.{Datahub, Entity, RemoteDatasource, Subscriber}
import com.github.oseval.datahub.remote.RemoteSubscriber.{SubsData, SubsOps}

import scala.concurrent.Future
import scala.ref.WeakReference

/**
  * This is one of the remotes interactions parts
  *
  * LocalSubscriber -> Datahub -> RemoteDatasource - - -> RemoteSubscriber -> Datahub -> LocalDatasource
  */
object RemoteDatasourceConnector {
  trait SubscriptionStorage {
    /**
      * Compose data with a given update and persist it to storage
      * @param data
      */
    def onUpdate(update: SubsData): Unit

    /**
      * Load data from storage. Should return SubsOps.zero if none.
      * @return
      */
    def loadData(): Future[SubsData]
  }

  trait SubscriptionsManagement { this: RemoteDatasource =>
    /**
      * Storage to save data in case of local datahub accidentally crashed.
      * Doesn't require any saving guarantees or transactions
      */
    def subscriptionStorage: SubscriptionStorage

    /**
      * Current subscriptions
      */
    private val subscriptions = new AtomicReference(SubsOps.zero)

    def onSubscriptionsLoaded(subs: SubsOps.D): Unit =
      subscriptions.accumulateAndGet(subs, { (curData: SubsOps.D, _) =>
        SubsOps.merge(curData, subs)
      })

    /**
      * Calls by source when subscriptions are updated to send diff to RemoteSubscriber
      * @param update
      */
    protected def updateSubscriptions(update: SubsOps.D): Unit

    /**
      * Calls from remote to sync subscriptions
      * @param clock
      */
    def syncSubscriptions(clock: Long): Unit =
      updateSubscriptions(SubsOps.diffFromClock(subscriptions.get(), clock))

    override def onSubscribe(entity: Entity,
                             subscriber: Subscriber,
                             lastKnownDataClock: Any
                            ): Unit =
      subscriptions.accumulateAndGet(SubsOps.zero,  { (curData: SubsOps.D, _) =>
        val newClock = System.currentTimeMillis
        val updatedSubs = curData.data.subs.add(entity -> lastKnownDataClock, newClock)
        val newData = curData.updated(curData.data.copy(subs = updatedSubs, clock = newClock))

        val diff = SubsOps.diffFromClock(newData, curData.clock)
        updateSubscriptions(diff)
        subscriptionStorage.onUpdate(newData.data)

        newData
      })

    override def onUnsubscribe(entity: Entity, subscriber: Subscriber): Unit = {
      subscriptions.accumulateAndGet(SubsOps.zero,  { (curData: SubsOps.D, _) =>
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
  * LocalSubscriber -> Datahub -> RemoteDatasource - - -> RemoteSubscriber -> Datahub -> LocalDatasource
  */
trait RemoteDatasourceConnector extends RemoteDatasource with SubscriptionsManagement {
  protected val datahub: WeakReference[Datahub]

  /**
    * Calls by external connection when data update comes from remote
    * @param entity
    * @param data
    */
  def onUpdate(entityId: String, data: Data): Unit =
    datahub().dataUpdated(entityId, data)
}