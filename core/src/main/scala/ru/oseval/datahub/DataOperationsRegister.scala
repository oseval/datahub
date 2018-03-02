package ru.oseval.datahub

import ru.oseval.datahub.data.DataOps

import scala.collection.concurrent.TrieMap

object DataOperationsRegistry {
  private val reg = TrieMap.empty[String, DataOps]
  def register(ops: DataOps): Unit = reg.update(ops.kind, ops)
  def getOps(kind: String): DataOps =
    reg.getOrElse(kind, throw new NoSuchElementException("No data ops for kind " + kind))
}
