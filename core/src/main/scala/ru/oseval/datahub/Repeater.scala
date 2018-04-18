package ru.oseval.datahub

import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

case class RepeaterConfig(base: Long, maxDelay: Long)

case class Repeater(name: String, config: RepeaterConfig, scheduleOnce: (Long, () => Any) => Unit, log: Logger) {
  def run[T](fut: () => Future[T], attempt: Int = 0, p: Promise[T] = Promise[T])
            (implicit ec: ExecutionContext): Future[T] = {
    fut().andThen {
      case Success(r) => p.success(r)
      case Failure(e) =>
        val nextAttempt = attempt + 1
        val delay = Math.pow(config.base.toDouble, nextAttempt.toDouble) min config.maxDelay
        log.warn(
          "Future inside repeater {} failed with {} and will be recovered with attempt is {} and delay is {}",
          name, e.getMessage, nextAttempt.toString, delay.toString
        )

        scheduleOnce(delay.toLong, () => run(fut, nextAttempt, p))
    }
    p.future
  }
}