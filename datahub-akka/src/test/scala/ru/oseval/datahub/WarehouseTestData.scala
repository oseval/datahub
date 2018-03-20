package ru.oseval.datahub

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import ru.oseval.datahub.ProductTestData._
import akka.pattern.ask
import akka.util.Timeout
import org.slf4j.LoggerFactory
import ru.oseval.datahub.ActorProductTestData.{Ping, Pong}

import scala.concurrent.Future
import scala.concurrent.duration._

object ActorWarehouseTestData {
  import WarehouseTestData._

  def warehouseProps(warehouseId: WarehouseId, notifier: ActorRef): Props =
    Props(classOf[WarehouseDataHolder], warehouseId, notifier)

  case object GetProducts
  case class AddProduct(id: ProductId)

  class WarehouseDataHolder(warehouseId: WarehouseId, protected val datahub: Datahub[Future])
    extends ActorDataMethods[Future] with Actor with ActorLogging {
    private implicit val timeout: Timeout = 3.seconds

    private val warehouse = WarehouseEntity(warehouseId)
    protected val storage = new LocalDataStorage(
      LoggerFactory.getLogger("warehouse"),
      ActorFacade(_, self), datahub
    )

    storage.addEntity(warehouse)(warehouse.ops.zero)

    override def receive: Receive = handleDataMessage(warehouse) orElse {
      case Ping => sender() ! Pong
      case GetProducts => sender() ! storage.get(warehouse).toSeq.flatMap(_.elements)
      case AddProduct(productId) =>
        val product = ProductEntity(productId)
        storage.addRelation(product)
        storage.updateEntity(warehouse)(int => _.updated(productId, int.cur))
    }
  }
}
