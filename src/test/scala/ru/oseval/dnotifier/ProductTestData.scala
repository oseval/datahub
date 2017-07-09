package ru.oseval.dnotifier

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.event.LoggingAdapter
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import scala.concurrent.Future
import scala.concurrent.duration._

object ProductTestData {
  type ProductId = String
  type ProductClock = String

  case class ProductData(name: String, amount: Int, lastUpdated: Long) extends Data { self ⇒
    override val clock: ProductClock = lastUpdated.toString
  }

  object ProductOps extends DataOps {
    type DInner = ProductData

    override val zero: ProductData = ProductData("", 0, 0L)

    override val ordering: Ordering[ProductClock] = Data.timestampOrdering

    override def combine(a: ProductData, b: ProductData): ProductData =
      if (ordering.gt(a.clock, b.clock)) a else b

    override def diffFromClock(data: ProductData, from: ProductClock): ProductData = data

    override def getRelatedEntities(data: ProductData): Set[String] = Set.empty
  }

  case class ProductFacade(id: ProductId, holder: ActorRef) extends ActorFacade {
    val ops = ProductOps
  }

  class Product(val id: ProductId,
                val notifyDataUpdated: (Notifier.NotifyDataUpdated) => Future[Unit]
               ) extends AbstractEntity(ProductOps.zero) {
    val ops = ProductOps
  }

  def productProps(productId: String, notifier: ActorRef): Props = Props(classOf[ProductDataHolder], productId, notifier)

  case object Ping
  case object Pong
  case class UpdateData(data: ProductData)

  private class ProductDataHolder(productId: String, protected val notifier: ActorRef)
    extends Actor with ActorLogging with ActorDataMethods {
    import context.dispatcher
    private implicit val timeout: Timeout = 3.seconds
    private val product = new Product(productId, notifier.ask(_).mapTo[Unit])

    override def receive: Receive = handleDataMessage(product) orElse {
      case Ping ⇒ sender() ! Pong
      case UpdateData(updated) ⇒ product.combine(updated) pipeTo sender()
    }
  }
}