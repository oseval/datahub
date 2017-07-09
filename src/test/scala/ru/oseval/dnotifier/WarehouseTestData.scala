package ru.oseval.dnotifier

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.event.LoggingAdapter
import ru.oseval.dnotifier.ProductTestData._
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

object WarehouseTestData {
  type WarehouseId = String
  type WarehouseClock = String

  case class WarehouseData(products: Map[WarehouseClock, ProductId]) extends Data { self ⇒
    override val clock: WarehouseClock = if (products.isEmpty) "0" else products.keySet.maxBy(_.toLong)
  }

  object WarehouseOps extends DataOps {
    type DInner = WarehouseData

    override val zero: WarehouseData = WarehouseData(Map.empty)

    override val ordering: Ordering[WarehouseClock] = new Ordering[WarehouseClock] {
      private val impl = implicitly[Ordering[Long]]
      override def compare(x: WarehouseClock, y: WarehouseClock): Int = impl.compare(x.toLong, y.toLong)
    }

    override def combine(a: WarehouseData, b: WarehouseData): WarehouseData =
      WarehouseData(a.products ++ b.products)

    override def diffFromClock(data: WarehouseData, from: WarehouseClock): WarehouseData =
      WarehouseData(products = data.products.filterKeys(ordering.gt(_, from)))

    override def getRelatedEntities(data: WarehouseData): Set[ProductId] =
      data.products.values.toSet
  }

  class Warehouse(val id: WarehouseId,
                  val notifyDataUpdated: (Notifier.NotifyDataUpdated) => Future[Unit],
                  val log: LoggingAdapter)
    extends AbstractEntity(WarehouseOps.zero) {
    val ops = WarehouseOps

    private val products = mutable.Map.empty[ProductId, ProductData]

    override def relatedDataUpdated(relatedId: String, related: Data): Unit = related match {
      case newData: ProductData if products contains relatedId ⇒
        products.update(relatedId, ProductOps.combine(products(relatedId), newData))
      case _ ⇒
        log.warning("Uhandled data update from stream " + relatedId)
    }
  }

  case class WarehouseFacade(id: WarehouseId, holder: ActorRef) extends ActorFacade {
    val ops = WarehouseOps
  }

  def warehouseProps(warehouseId: WarehouseId, notifier: ActorRef): Props =
    Props(classOf[WarehouseDataHolder], warehouseId, notifier)

  case object GetProducts
  case class AddProduct(id: ProductId)

  class WarehouseDataHolder(warehouseId: WarehouseId, protected val notifier: ActorRef)
    extends ActorDataMethods with Actor with ActorLogging {
    private implicit val timeout: Timeout = 3.seconds

    private val warehouse = new Warehouse(warehouseId, notifier.ask(_).mapTo[Unit], log)

    override def receive: Receive = handleDataMessage(warehouse) orElse {
      case Ping ⇒ sender() ! Pong
      case GetProducts ⇒ sender() ! warehouse.data.products.toMap
      case AddProduct(productId) ⇒
        warehouse.combine(WarehouseData(products = Map(System.currentTimeMillis.toString → productId)))
    }
  }
}
