package com.github.oseval.datahub.remote

import java.util.concurrent.atomic.AtomicReference

import com.github.oseval.datahub.data.InferredOps.InferredOps
import com.github.oseval.datahub.data._
import com.github.oseval.datahub._

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/**
  * This is one of the remotes interactions parts
  *
  * LocalSubscriber -> Datahub -> RemoteFacade - - -> RemoteSubscriber -> Datahub -> LocalFacade
  */
object RemoteSubscriber {
  case class SubsData(subs: SetData[(Entity, Any) /** entityId / clock **/, Long],
                      clock: Long
                     ) extends Data {
    lazy val elemMap: Map[Entity, Any] = subs.elements.toMap
    override type C = Long
  }

  object SubsOps extends ALODataOps[InferredOps[SubsData]] {
    override protected val ops: InferredOps[SubsData] = InferredOps(
      SubsData(SetDataOps.zero[(Entity, Any), Long], 0L),
      "subscriptions"
    )
  }

  trait SubscriptionsManagement { this: Subscriber =>
    protected implicit val ec: ExecutionContext
    // TODO: Must it be the SoftReference?
    protected val datahub: Datahub

    private val subscriptions: AtomicReference[ALOData[SubsData]] = new AtomicReference(SubsOps.zero)
    private val pendingSubscriptions: TrieMap[Entity, Boolean] = TrieMap.empty

    private def addSubscription(relation: Entity, lastKnownClock: Any): Unit =
      if (!datahub.subscribe(relation, this, lastKnownClock))
        pendingSubscriptions.put(relation, true)

    protected def syncSubscriptions(clock: Long): Unit

    /**
      * Calls by external channel provider
      * @param update
      */
    def onSubscriptionsUpdate(update: ALOData[SubsData]): Unit = {
      val beforeClock = subscriptions.get().clock
      val newData = subscriptions.accumulateAndGet(update, SubsOps.combine)
      if (!newData.isSolid) syncSubscriptions(newData.clock)

      val realUpdate = SubsOps.diffFromClock(newData, beforeClock)
      val addedSubscriptions = realUpdate.data.subs.elements
      val removedSubscriptions = realUpdate.data.subs.removedElements

      addedSubscriptions.foreach { case (relation, clock) =>
        addSubscription(relation, clock)
      }

      removedSubscriptions.foreach { case (relation, clock) =>
        datahub.unsubscribe(relation, this)
      }
    }

    /**
      * Calls of this method must be scheduled with an interval
      * @return
      */
    def checkDataIntegrity: Boolean = {
      val subscriptionsIsSolid = subscriptions.get().isSolid && subscriptions.get().clock != SubsOps.zero.clock

      syncSubscriptions(subscriptions.get().clock)

      pendingSubscriptions.foreach { case (relation, _) =>
        if (datahub.subscribe(relation, this, relation.ops.zero.clock)) {
          pendingSubscriptions -= relation
        }
      }

      subscriptionsIsSolid && pendingSubscriptions.isEmpty
    }
  }
}
import RemoteSubscriber._

/**
  * This is one of the remotes interactions parts
  *
  * LocalSubscriber -> Datahub -> RemoteFacade - - -> RemoteSubscriber -> Datahub -> LocalFacade
  */
trait RemoteSubscriber extends Subscriber with SubscriptionsManagement {
  /**
    * Called from outside by the remote request
    * @param entity
    * @param dataClock
    * @return
    */
  def syncData(entity: Entity, dataClock: Any): Unit =
    datahub.syncRelationClocks(this, Map(entity -> dataClock))
}