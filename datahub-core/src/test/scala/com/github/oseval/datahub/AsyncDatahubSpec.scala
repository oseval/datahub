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
    val datasource = mock[LocalDatasource]
    val datahub = createDatahub

    when(datasource.entity).thenReturn(ProductEntity(1))

    datahub.register(datasource)
  }

  it should "subscribe on related data entities" in {
    val datahub = createDatahub()

    val product = ProductEntity(1)
    val productSource = mock[LocalDatasource { val entity: product.type }]
    when(productSource.entity).thenReturn(product)
    val productData = ProductOps.zero

    val newProductData = ProductData("TV", 1, System.currentTimeMillis)

    val subscriber = mock[Subscriber]
//    when(productSource.syncData(productData.clock)).thenReturn()

    datahub.register(productSource)
    datahub.subscribe(product, subscriber, ProductOps.zero.clock)

    eventually {
      verify(productSource).syncData(productData.clock)
    }

    // Product entity data is updated
    datahub.dataUpdated(product)(newProductData)

    eventually {
      verify(productSource).syncData(productData.clock)
      verify(subscriber).onUpdate(product)(newProductData)
    }
  }
}
