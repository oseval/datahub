package ru.oseval.datahub

import ru.oseval.datahub.data.DataOps

import scala.collection.concurrent.TrieMap

object DataEntityRegistry {
  private val reg = TrieMap.empty[String, String => Entity]
  def register(kind: String, constructor: String => Entity): Unit = reg.update(kind, constructor)
  def getConstructor(kind: String): String => Entity =
    reg.getOrElse(kind, throw new NoSuchElementException("No entity constructor data ops for kind " + kind))
}
