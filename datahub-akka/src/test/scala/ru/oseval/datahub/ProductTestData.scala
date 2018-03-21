package ru.oseval.datahub

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._

object ActorProductTestData {
  import ProductTestData._

  def productProps(productId: Int, datahub: Datahub[Future]): Props =
    Props(classOf[ProductActor], productId, datahub)

  case object Ping
  case object Pong
  case class UpdateData(data: ProductData)

  private class ProductActor(productId: Int, protected val datahub: Datahub[Future])
    extends Actor with ActorLogging with ActorDataMethods[Future] {
    import context.dispatcher
    private implicit val timeout: Timeout = 3.seconds
    private val product = ProductEntity(productId)
    protected val storage = new LocalDataStorage(
      LoggerFactory.getLogger("product"), ActorFacade(_, self), datahub
    )

    storage.addEntity(product)(product.ops.zero)

    override def receive: Receive = handleDataMessage(product) orElse {
      case Ping =>
        println(("PPP", sender()))
        sender() ! Pong
      case UpdateData(updated) =>
        storage.updateEntity(product)(_ => _ => updated) pipeTo sender()
    }
  }
}