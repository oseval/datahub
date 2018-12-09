package ru.oseval.datahub

import org.scalatest
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.mockito.Mockito._

import scala.concurrent.duration._
import ru.oseval.datahub.domain.ProductTestData._
import ru.oseval.datahub.domain.WarehouseTestData._
import org.scalatest.mockito.MockitoSugar
import ru.oseval.datahub
import ru.oseval.datahub.data.{ALOData, ClockInt}

import scala.concurrent.Future
import scala.util.Random

class DataUsageSpec extends FlatSpecLike
  with BeforeAndAfterAll
  with MockitoSugar
  with ScalaFutures
  with scalatest.Matchers
  with Eventually
  with CommonTestMethods {

  // to simulate network errors
  val flawedDatahub = new AsyncDatahub() {
    private def withFlaw[T](f: () => T): T =
      if (Random.nextInt % 2 == 1) throw new Exception("Datahub call was flawed")
      else f()
    override def register(facade: EntityFacade): Unit =
      withFlaw(() => super.register(facade))
    override def dataUpdated(entity: Entity)(data: entity.ops.D): Unit =
      withFlaw(() => super.dataUpdated(entity)(data))
    override def subscribe(entity: Entity, // this must be entity to get ops and compare clocks
                           subscriber: Subscriber,
                           lastKnownDataClock: Any): Boolean =
      withFlaw(() => super.subscribe(entity, subscriber, lastKnownDataClock))
    override def unsubscribe(entity: Entity, subscriber: Subscriber): Unit =
      withFlaw(() => super.unsubscribe(entity, subscriber))
  }

  behavior of "Data"

  it should "" in {

  }
}