package com.github.oseval.datahub

import org.scalatest.{FlatSpecLike, Matchers}
import org.scalatest.concurrent.Eventually
import org.mockito.Mockito._
import com.github.oseval.datahub.domain.ProductTestData._
import com.github.oseval.datahub.data.SetData
import org.mockito.Mockito.spy
import com.github.oseval.datahub.domain.WarehouseTestData.{WarehouseData, WarehouseEntity, WarehouseOps}

import scala.util.Try
import scala.concurrent.duration._

class RemoteSpec extends FlatSpecLike with CommonTestMethods with Eventually with Matchers {
  import org.mockito.Matchers._

  implicit val cfg = PatienceConfig(2.seconds)

  val product1 = ProductEntity(1)

  val warehouse1 = WarehouseEntity("1")
  val time = System.currentTimeMillis
  val warehouseData1 = WarehouseOps.zero.updated(WarehouseData(SetData.one(1, time)))
  val warehouseData2 = warehouseData1.updated(WarehouseData(SetData(1, 2)(time)(_ + 1)))

  behavior of "Remote datahub"

  /**
    * LocalSubscriber -> Datahub -> RemoteDatasource - - -> RemoteSubscriber -> Datahub -> LocalDatasource
    */

  it should "subscribe on remote" in {
    subscriberOnRemote(product1)
  }

  it should "replicate data consistently" in {
    val (clientDH, clientSource, serverDH, serverSubscriber, subs, _) = subscriberOnRemote(warehouse1)

    // This update will be sent to the client from the server
    val update = WarehouseOps.diffFromClock(warehouseData2, warehouseData1.clock)
    eventually {
      serverDH.dataUpdated(warehouse1)(update)
      verify(subs, atLeastOnce()).onUpdate(warehouse1.id, update)
    }

    eventually {
      // On a client we are syncing data, because it is not solid
      clientDH.syncRelationClocks(subs, Map(warehouse1 -> warehouse1.ops.zero.clock))
      verify(serverSubscriber, atLeastOnce()).syncData(warehouse1, warehouse1.ops.zero.clock)
    }
  }

  private def subscriberOnRemote[E <: Entity](entity: E) = {
    val (clientDH, clientSource, serverDH, serverSubscriber) = makeRemotes(entity.ops)

    val subs = spy[Subscriber](new SpiedSubscriber)
    val localZeroSource = spy[LocalZeroDatasource[E]](LocalZeroDatasource(entity, clientDH))

    // register the Entity datasource to make it possible to subscribe on Product
    // this method doesn't interact with the weak transport
    serverDH.register(localZeroSource)

    // Register remote client datasource which will subscribe on remote entities
    // this method doesn't interact with the weak transport
    clientDH.register(clientSource)

    // Subscribe local subscriber on Entity - add subscription to the Remote client datasource state
    // This state synchronized by the Remote server subscriber checkDataIntegrity method
    // this call interacts with the weak transport
    Try(clientDH.subscribe(entity, subs, entity.ops.zero.clock))

    // waiting while subscriptions from the Remote client datasource will be fully replicated to the Remote server subscriber
    eventually(serverSubscriber.checkDataIntegrity shouldBe true)
    verify(serverSubscriber, atLeastOnce()).onSubscriptionsUpdate(any())

    // When subscriptions are fully synchronized then the Remote server subscriber is subscribed on the Product
    verify(serverDH, atLeastOnce()).subscribe(entity, serverSubscriber, entity.ops.zero.clock)

    (clientDH, clientSource, serverDH, serverSubscriber, subs, localZeroSource)
  }
}
