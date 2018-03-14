package ru.oseval.datahub

import scala.language.implicitConversions

trait TaggedType[R, A] {
  type T = R with A
  implicit def apply(v: A): T = v.asInstanceOf[T]
}
