package com.github.oseval.datahub

import org.slf4j.LoggerFactory
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import org.mockito.Mockito.spy
import org.scalatest.mockito.MockitoSugar
import com.github.oseval.datahub.data.{ALOData, Data, DataOps}
import com.github.oseval.datahub.domain.ProductTestData.{ProductClock, ProductEntity, ProductOps}
import com.github.oseval.datahub.remote.RemoteSubscriber.SubsOps
import com.github.oseval.datahub.remote.{RemoteDatasourceConnector, RemoteSubscriber}
import org.scalatest
import com.github.oseval.datahub.domain.WarehouseTestData.{WarehouseEntity, WarehouseOps}
import com.github.oseval.datahub.remote.RemoteDatasource.SubscriptionStorage

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.ref.WeakReference
import scala.util.{Random, Try}

trait CommonTestMethods extends MockitoSugar with scalatest.Matchers {
  protected val log = LoggerFactory.getLogger(getClass)

  protected implicit val ec = scala.concurrent.ExecutionContext.global
  protected implicit val timeout = 3.seconds

  DataEntityRegistry.register(ProductOps.kind, { entityId =>
    ProductEntity(entityId.split('_')(1).toInt)
  })
  DataEntityRegistry.register(WarehouseOps.kind, { entityId =>
    WarehouseEntity(entityId.split('_')(1))
  })

  protected val scheduler = new ScheduledThreadPoolExecutor(1)
  protected def scheduleOnce(delay: Long, f: () => Any): Unit =
    scheduler.schedule(new Runnable {
      override def run(): Unit = f()
    }, delay, TimeUnit.MILLISECONDS)

  class WeakTransort {
    def push[A, B](t: A, f: => B): B =
      if (Random.nextLong % 3 != 0) f
      else throw new RuntimeException("Transport failure for push " + t)
  }
  val weakTransport: WeakTransort = new WeakTransort

  def inMemoryStorage() = new SubscriptionStorage {
    override def onUpdate(update: RemoteSubscriber.SubsData): Unit = ()
    override def loadData(): Future[RemoteSubscriber.SubsData] = Future.successful(SubsOps.zero.data)
  }

  def createDatahub() = new AsyncDatahub()(ec)


  class SpiedSubscriber extends Subscriber {
    override def onUpdate(relation: Entity)(relationData: relation.ops.D): Unit = ()
    override def onUpdate(relationId: String , relationData: Data): Unit = ()
  }

  class SpiedDatahub extends Datahub {
    override def register(source: Datasource): Unit = ()
    override def subscribe(entity: Entity,
                           subscriber: Subscriber,
                           lastKnownDataClock: Any): Boolean = true
    override def unsubscribe(entity: Entity, subscriber: Subscriber): Unit = ()
    override def dataUpdated(entity: Entity)(data: entity.ops.D): Unit = ()
    override def dataUpdated(entityId: String, data: Data): Unit = ()
    override def syncRelationClocks(entity: Subscriber,
                                    relationClocks: Map[Entity, Any]): Unit = ()
  }

  case class LocalZeroDatasource[E <: Entity](entity: E, dh: Datahub) extends LocalDatasource {
    override def syncData(dataClock: entity.ops.D#C): Unit =
      dh.dataUpdated(entity)(entity.ops.zero)
  }

  class SpiedRemoteDatasourceConnector(val ops: DataOps,
                                       val datahub: WeakReference[Datahub],
                                       rs: => RemoteSubscriber) extends RemoteDatasourceConnector {
    override val subscriptionStorage: SubscriptionStorage = inMemoryStorage()
    override protected def updateSubscriptions(update: ALOData[RemoteSubscriber.SubsData]): Unit =
      Try(weakTransport.push("RemoteDatasource_updateSubscriptions", rs.onSubscriptionsUpdate(update)))

    override def syncData(entityId: String, dataClock: ops.D#C): Unit = {
      val entity = DataEntityRegistry.getConstructor(ops.kind)(entityId)
      weakTransport.push("RemoteDatasource_syncData", rs.syncData(entity, dataClock))
    }
  }

  class SpiedRemoteSubscriber(val datahub: Datahub, rf: => RemoteDatasourceConnector) extends RemoteSubscriber {
    override protected def syncSubscriptions(clock: ProductClock): Unit =
      weakTransport.push("RemoteSubscriber_synSubscriptions", rf.syncSubscriptions(clock))
    override def onUpdate(relationId: String, relationData: Data): Unit =
      Try(weakTransport.push("RemoteSubscriber_onUpdate", rf.onUpdate(relationId, relationData)))
    override def onUpdate(relation: Entity)(relationData: relation.ops.D): Unit =
      onUpdate(relation.id, relationData)
  }

  def spiedDatahub(): Datahub = {
    val datahub = new SpiedDatahub
    spy[SpiedDatahub](datahub)
  }

  def makeLocalStorage(knownData: Map[Entity, Data] = Map.empty): (LocalDataStorage, Datahub) = {
    val datahub = new SpiedDatahub
    val spiedhub = spy[SpiedDatahub](datahub)
    new LocalDataStorage(WeakReference(spiedhub), _ => null, LoggerFactory.getLogger(getClass), (_, _) => (), knownData) -> spiedhub
  }

  def makeRemotes(_ops: DataOps) = {
    val localDH = spy[AsyncDatahub](new AsyncDatahub())
    lazy val rf: SpiedRemoteDatasourceConnector =
      spy[SpiedRemoteDatasourceConnector](new SpiedRemoteDatasourceConnector(_ops, WeakReference(localDH), rs))

    lazy val remoteDH = spy[AsyncDatahub](new AsyncDatahub())
    lazy val rs: RemoteSubscriber =
      spy[RemoteSubscriber](new SpiedRemoteSubscriber(remoteDH, rf))

    (localDH, rf, remoteDH, rs)
  }
}
