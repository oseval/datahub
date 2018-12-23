package com.github.oseval.datahub

import org.scalatest
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.mockito.Mockito._

import com.github.oseval.datahub.domain.ProductTestData._
import org.scalatest.mockito.MockitoSugar
import com.github.oseval.datahub.data.ALOData

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
    val facade = mock[LocalEntityFacade]
    val datahub = createDatahub

    when(facade.entity).thenReturn(ProductEntity(1))

    datahub.register(facade)
  }

  it should "subscribe on related data entities" in {
    val datahub = createDatahub()

    val product = ProductEntity(1)
    val productFacade = mock[LocalEntityFacade { val entity: product.type }]
    when(productFacade.entity).thenReturn(product)
    val productData = ProductOps.zero

    val newProductData = ProductData("TV", 1, System.currentTimeMillis)

    val subscriber = mock[Subscriber]
//    when(productFacade.syncData(productData.clock)).thenReturn()

    datahub.register(productFacade)
    datahub.subscribe(product, subscriber, ProductOps.zero.clock)

    eventually {
      verify(productFacade).syncData(productData.clock)
    }

    // Product entity data is updated
    datahub.dataUpdated(product)(newProductData)

    eventually {
      verify(productFacade).syncData(productData.clock)
      verify(subscriber).onUpdate(product)(newProductData)
    }
  }
}
