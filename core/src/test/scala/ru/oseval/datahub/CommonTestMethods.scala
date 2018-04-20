package ru.oseval.datahub

import org.slf4j.LoggerFactory

import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import scala.concurrent.duration._

trait CommonTestMethods {
  protected val log = LoggerFactory.getLogger(getClass)

  protected implicit val ec = scala.concurrent.ExecutionContext.global
  protected implicit val timeout = 3.seconds

  protected val scheduler = new ScheduledThreadPoolExecutor(1)
  protected def scheduleOnce(delay: Long, f: () => Any): Unit =
    scheduler.schedule(new Runnable {
      override def run(): Unit = f()
    }, delay, TimeUnit.MILLISECONDS)
  protected val repeater = Repeater("TestRepeater", RepeaterConfig(500, 5000), scheduleOnce, log)
  def createDatahub = new AsyncDatahub(new MemoryStorage, repeater)(ec)
}
