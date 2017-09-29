package ru.oseval.datahub

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

object ActorProductTestData {
  import ProductTestData._

  def productProps(productId: Int, notifier: ActorRef): Props =
    Props(classOf[ProductActor], productId, notifier)

  case object Ping
  case object Pong
  case class UpdateData(data: ProductData)

  private class ProductActor(productId: Int, protected val notifier: ActorRef)
    extends Actor with ActorLogging with ActorDataMethods {
    import context.dispatcher
    private implicit val timeout: Timeout = 3.seconds
    private val product = ProductEntity(productId)
    protected val storage = new LocalDataStorage(
      LoggerFactory.getLogger("product"), ActorFacade(_, self), notifier.ask(_).mapTo[Unit]
    )

    storage.addEntity(product)(product.ops.zero)

    override def receive: Receive = handleDataMessage(product) orElse {
      case Ping => sender() ! Pong
      case UpdateData(updated) =>
        storage.updateEntity(product)(_ => _ => updated) pipeTo sender()
    }
  }
}