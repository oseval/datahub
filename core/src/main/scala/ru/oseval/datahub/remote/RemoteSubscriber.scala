package ru.oseval.datahub.remote

import java.util.concurrent.atomic.AtomicReference

import ru.oseval.datahub.data.InferredOps.InferredOps
import ru.oseval.datahub.data._
import ru.oseval.datahub._

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

object RemoteDatahub {
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
}
import RemoteDatahub._


trait RemoteSubscriber extends Subscriber {
  val datahub: Datahub
  protected implicit val ec: ExecutionContext
  private val subscriptions: AtomicReference[ALOData[SubsData]] = new AtomicReference(SubsOps.zero)
  private val pendingSubscriptions: TrieMap[Entity, Boolean] = TrieMap.empty

  private def addSubscription(relation: Entity, lastKnownClock: Any): Unit =
    if (!datahub.subscribe(relation, this, lastKnownClock))
      pendingSubscriptions.put(relation, true)

  /**
    * Called from outside subscriber by the remote request
    * @param clock
    * @return
    */
  protected def getUpdatesFrom(clock: Long): Future[ALOData[SubsData]]

  /**
    * Calls by external channel provider
    * @param update
    */
  def onSubscriptionsUpdate(update: ALOData[SubsData]): Unit = {
    val beforeClock = subscriptions.get().clock
    val newData = subscriptions.accumulateAndGet(update, SubsOps.combine)
    if (!newData.isSolid) getUpdatesFrom(newData.clock).foreach(onSubscriptionsUpdate)

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
    if (!subscriptions.get().isSolid) getUpdatesFrom(subscriptions.get().clock)

    pendingSubscriptions.foreach { case (relation, _) =>
      if (datahub.subscribe(relation, this, relation.ops.zero.clock)) pendingSubscriptions -= relation
    }

    subscriptions.get().isSolid && pendingSubscriptions.isEmpty
  }
}

trait RemoteDatahub extends Datahub {
  val datahub: Datahub

  /**
    * Storage to save data in case of local datahub crashed
    */
  val subscriptionStorage: SubscriptionStorage
  val subscriptions = new AtomicReference(SubsOps.zero)

  def updateSubscriptions(update: SubsOps.D): Unit

  override def register(facade: EntityFacade): Unit = datahub.register(facade)

  def getUpdatesFrom(clock: Long): SubsOps.D =
    SubsOps.diffFromClock(subscriptions.get(), clock)

  override def subscribe(entity: Entity,
                         subscriber: Subscriber,
                         lastKnownDataClock: Any
                        ): Boolean = {
    subscriptions.accumulateAndGet(SubsOps.zero,  { (_, curData: SubsOps.D) =>
        val newClock = System.currentTimeMillis
        val updatedSubs = curData.data.subs.add(entity -> lastKnownDataClock, newClock)
        val newData = curData.updated(curData.data.copy(subs = updatedSubs, clock = newClock))

        val diff = SubsOps.diffFromClock(newData, curData.clock)
        updateSubscriptions(diff)
        subscriptionStorage.onUpdate(newData.data)

        newData
    })

    datahub.subscribe(entity, subscriber, lastKnownDataClock)
  }

  override def unsubscribe(entity: Entity, subscriber: Subscriber): Unit = {
    datahub.unsubscribe(entity, subscriber)

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


trait SubscriptionStorage {
  /**
    * Compose data with a given update and persist it to storage
    * @param data
    */
  def onUpdate(update: SubsData): Unit

  /**
    * Load data from storage
    * @return
    */
  def loadData(): Future[SubsData]
}