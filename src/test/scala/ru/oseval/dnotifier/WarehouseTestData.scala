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

  object WarehouseOps extends DataOps[WarehouseData] {
    override val ordering: Ordering[WarehouseClock] = Data.timestampOrdering
    override val zero: WarehouseData = WarehouseData(Map.empty)

    override def combine(a: WarehouseData, b: WarehouseData): WarehouseData =
      WarehouseData(a.products ++ b.products)

    override def diffFromClock(data: WarehouseData, from: WarehouseClock): WarehouseData =
      WarehouseData(products = data.products.filterKeys(ordering.gt(_, from)))

    override def getRelations(data: WarehouseData): Set[ProductId] =
      data.products.values.toSet

    override def makeId(ownId: Any): String = "warehouse_" + ownId
  }

  case class WarehouseEntity(ownId: String) extends Entity {
    override type D = WarehouseData
    val ops = WarehouseOps
  }

  def warehouseProps(warehouseId: WarehouseId, notifier: ActorRef): Props =
    Props(classOf[WarehouseDataHolder], warehouseId, notifier)

  case object GetProducts
  case class AddProduct(id: ProductId)

  class WarehouseDataHolder(warehouseId: WarehouseId, protected val notifier: ActorRef)
    extends ActorDataMethods with Actor with ActorLogging {
    private implicit val timeout: Timeout = 3.seconds

    private val warehouse = WarehouseEntity(warehouseId)
    protected val storage = new LocalDataStorage(log, ActorFacade(_, self), notifier.ask(_).mapTo[Unit])

    storage.addEntity(warehouse)(warehouse.ops.zero)

    override def receive: Receive = handleDataMessage(warehouse) orElse {
      case Ping ⇒ sender() ! Pong
      case GetProducts ⇒ sender() ! storage.get(warehouse).toSeq.flatMap(_.products).toMap
      case AddProduct(productId) ⇒
        val product = ProductEntity(productId)
        storage.addRelation(product)
        storage.combine(warehouse)(WarehouseData(products = Map(System.currentTimeMillis.toString → product.id)))
    }
  }
}
