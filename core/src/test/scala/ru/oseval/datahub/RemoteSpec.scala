package ru.oseval.datahub

import org.scalatest
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.mockito.Mockito._
import ru.oseval.datahub.domain.ProductTestData._
import org.scalatest.mockito.MockitoSugar
import org.slf4j.LoggerFactory
import ru.oseval.datahub.data.{ALOData, Data}
import ru.oseval.datahub.domain.ProductTestData
import ru.oseval.datahub.remote.RemoteDatahub.SubsOps
import ru.oseval.datahub.remote.{RemoteDatahub, RemoteSubscriber, SubscriptionStorage}
import ru.oseval.datahub.utils.Commons.Id

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

class RemoteSpec extends FlatSpecLike with CommonTestMethods with Eventually with Matchers {

  behavior of "Remote datahub"

  it should "" in {
    val (rd, rs) = makeRemotes()

    val product1 = ProductEntity(1)
    val productData1 = ProductData("P1", 1, System.currentTimeMillis)
    val productData2 = ProductData("P2", 2, System.currentTimeMillis + 1)
    DataEntityRegistry.register(ProductOps.kind, id => ProductEntity(id.split('_')(1).toInt))

    val localStorage = new LocalDataStorage[Id](LoggerFactory.getLogger(getClass), _ => null, rd())

    eventually {
      rd().subscribe(product1, localStorage, ProductOps.zero.clock) shouldBe true
    }


    rs().onUpdate(product1)(productData1)

    eventually {

    }
  }
}
