package ru.oseval.datahub

import org.scalatest
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.mockito.Mockito._

import scala.concurrent.duration._
import ProductTestData._
import WarehouseTestData._
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
  val flawedDatahub = new AsyncDatahub(new MemoryStorage, repeater) {
    private def withFlaw(f: () => Future[Unit]): Future[Unit] =
      if (Random.nextInt % 2 == 1) Future.failed(new Exception("Datahub call was flawed"))
      else f()
    override def register(facade: EntityFacade)
                         (lastClock: facade.entity.ops.D#C,
                          relationClocks: Map[Entity, Any],
                          forcedSubscribers: Set[EntityFacade]): Future[Unit] =
      withFlaw(() => super.register(facade)(lastClock, relationClocks, forcedSubscribers))
    override def dataUpdated(entity: Entity, forcedSubscribers: Set[EntityFacade])
                            (data: entity.ops.D): Future[Unit] =
      withFlaw(() => super.dataUpdated(entity, forcedSubscribers)(data))
    override def subscribe(entity: Entity, // this must be entity to get ops and compare clocks
                           subscriber: Entity,
                           lastKnownDataClockOpt: Option[Any]): Future[Unit] =
      withFlaw(() => super.subscribe(entity, subscriber, lastKnownDataClockOpt))
    override def unsubscribe(entity: Entity, subscriber: Entity): Future[Unit] =
      withFlaw(() => super.unsubscribe(entity, subscriber))
  }

  behavior of "Data"

  it should "" in {

  }
}