package ru.oseval.datahub

import org.scalatest
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.mockito.Mockito._

import ru.oseval.datahub.domain.ProductTestData._
import org.scalatest.mockito.MockitoSugar
import ru.oseval.datahub.data.ALOData

import scala.concurrent.Future

class AsyncDatahubSpec extends FlatSpecLike
  with BeforeAndAfterAll
  with MockitoSugar
  with ScalaFutures
  with scalatest.Matchers
  with Eventually
  with CommonTestMethods {

  behavior of "Datahub"

  it should "register data entities" in {
    val facade = mock[EntityFacade]
    val datahub = createDatahub

    when(facade.entity).thenReturn(ProductEntity(1))

    datahub.register(facade)
  }

  it should "subscribe on related data entities" in {
    val datahub = createDatahub

    val product = ProductEntity(1)
    val productFacade = mock[EntityFacade { val entity: product.type }]
    when(productFacade.entity).thenReturn(product)
    val productData = ProductOps.zero

    val newProductData = ALOData(ProductData("TV", 1, System.currentTimeMillis))(0L)

    val subscriber = mock[Subscriber]
    when(productFacade.getUpdatesFrom(productData.clock)).thenReturn(Future.successful(productData))

    datahub.register(productFacade)
    datahub.subscribe(product, subscriber, ProductOps.zero.clock)

    eventually {
      verify(productFacade).getUpdatesFrom(productData.clock)
      verify(subscriber).onUpdate(product)(productData)
    }

    // Product entity data is updated
    datahub.dataUpdated(product)(newProductData)

    eventually {
      verify(productFacade).getUpdatesFrom(productData.clock)
      verify(subscriber).onUpdate(product)(newProductData)
    }
  }
}