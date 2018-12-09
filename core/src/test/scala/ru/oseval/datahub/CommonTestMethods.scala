package ru.oseval.datahub

import org.slf4j.LoggerFactory
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import org.mockito.Mockito.spy
import ru.oseval.datahub.data.{ALOData, Data}
import ru.oseval.datahub.domain.ProductTestData.ProductClock
import ru.oseval.datahub.remote.RemoteDatahub.SubsOps
import ru.oseval.datahub.remote.{RemoteDatahub, RemoteSubscriber, SubscriptionStorage}
import ru.oseval.datahub.utils.Commons.Id

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.ref.SoftReference
import scala.util.Random

trait CommonTestMethods {
  protected val log = LoggerFactory.getLogger(getClass)

  protected implicit val ec = scala.concurrent.ExecutionContext.global
  protected implicit val timeout = 3.seconds

  protected val scheduler = new ScheduledThreadPoolExecutor(1)
  protected def scheduleOnce(delay: Long, f: () => Any): Unit =
    scheduler.schedule(new Runnable {
      override def run(): Unit = f()
    }, delay, TimeUnit.MILLISECONDS)
//  protected val repeater = Repeater("TestRepeater", RepeaterConfig(500, 5000), scheduleOnce, log)

  class WeakTransort {
    def push[A, B](t: A, f: A => B): B = if (Random.nextLong % 3 == 0) f(t) else throw new RuntimeException("Transport failure for push " + t)
  }
  val weakTransort: WeakTransort = new WeakTransort

  def inMemoryStorage() = new SubscriptionStorage {
    override def onUpdate(update: RemoteDatahub.SubsData): Unit = ()
    override def loadData(): Future[RemoteDatahub.SubsData] = Future.successful(SubsOps.zero.data)
  }

  def createDatahub = new AsyncDatahub()(ec)



  class SpiedDatahub extends Datahub {
    override def register(facade: EntityFacade): Unit = ()
    override def subscribe(entity: Entity,
                           subscriber: Subscriber,
                           lastKnownDataClock: Any): Boolean = true
    override def unsubscribe(entity: Entity, subscriber: Subscriber): Unit = ()
    override def dataUpdated(entity: Entity)(data: entity.ops.D): Unit = ()
    override def syncRelationClocks(entity: Subscriber,
                                    relationClocks: Map[Entity, Any]): Unit = ()
  }

  def spiedDatahub(): Datahub = {
    val datahub = new SpiedDatahub
    spy[SpiedDatahub](datahub)
  }

  def makeLocalStorage(knownData: Map[Entity, Data] = Map.empty): (LocalDataStorage[Id], Datahub) = {
    val datahub = new SpiedDatahub
    val spiedhub = spy[SpiedDatahub](datahub)
    new LocalDataStorage[Id](LoggerFactory.getLogger(getClass), _ => null, spiedhub, knownData) -> spiedhub
  }



  def makeRemotes() = {
    lazy val rd: SoftReference[RemoteDatahub] = SoftReference(spy[RemoteDatahub](new RemoteDatahub {
      override val datahub: Datahub = spiedDatahub()
      override val subscriptionStorage: SubscriptionStorage = inMemoryStorage()
      override def updateSubscriptions(update: ALOData[RemoteDatahub.SubsData]): Unit =
        weakTransort.push(update, rs().onSubscriptionsUpdate)

      override def dataUpdated(entity: Entity)(data: entity.ops.D): Unit =
        datahub.dataUpdated(entity)(data)
      override def syncRelationClocks(subscriber: Subscriber, relationClocks: Map[Entity, Any]): Unit =
        datahub.syncRelationClocks(subscriber, relationClocks)
    }))

    lazy val rs: SoftReference[RemoteSubscriber] = SoftReference(spy[RemoteSubscriber](new RemoteSubscriber {
      override val datahub: Datahub = spiedDatahub()
      override protected implicit val ec: ExecutionContext = ExecutionContext.global

      override protected def getUpdatesFrom(clock: Long): Future[ALOData[RemoteDatahub.SubsData]] = {
        Future.successful(weakTransort.push(clock, rd().getUpdatesFrom))
      }

      override def onUpdate(relation: Entity)(relationData: relation.ops.D): Unit = {
        weakTransort.push((), (_: Unit) => rd().dataUpdated(relation)(relationData))
      }
    }))

    rd -> rs
  }
}