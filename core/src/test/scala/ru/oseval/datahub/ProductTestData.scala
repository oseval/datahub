package ru.oseval.datahub

import scala.concurrent.duration._

object ProductTestData {
  type ProductId = String
  type ProductClock = String

  case class ProductData(name: String, amount: Int, lastUpdated: Long) extends Data { self ⇒
    override val clock: ProductClock = lastUpdated.toString
  }

  object ProductOps extends DataOps[ProductData] {
    override val ordering: Ordering[ProductClock] = Data.timestampOrdering
    override val zero: ProductData = ProductData("", 0, 0L)

    override def combine(a: ProductData, b: ProductData): ProductData =
      if (ordering.gt(a.clock, b.clock)) a else b

    override def diffFromClock(data: ProductData, from: ProductClock): ProductData = data

    override def getRelations(data: ProductData): Set[String] = Set.empty

    override def makeId(ownId: Any): String = "product_" + ownId
  }

  case class ProductEntity(ownId: String) extends Entity {
    override type D = ProductData
    val ops = ProductOps
  }

  def productProps(productId: String, notifier: ActorRef): Props =
    Props(classOf[ProductActor], productId, notifier)

  case object Ping
  case object Pong
  case class UpdateData(data: ProductData)

  private class ProductActor(productId: String, protected val notifier: ActorRef)
    extends Actor with ActorLogging with ActorDataMethods {
    import context.dispatcher
    private implicit val timeout: Timeout = 3.seconds
    private val product = ProductEntity(productId)
    protected val storage = new LocalDataStorage(log, ActorFacade(_, self), notifier.ask(_).mapTo[Unit])

    storage.addEntity(product)(product.ops.zero)

    override def receive: Receive = handleDataMessage(product) orElse {
      case Ping => sender() ! Pong
      case UpdateData(updated) => storage.combine(product)(updated) pipeTo sender()
    }
  }
}