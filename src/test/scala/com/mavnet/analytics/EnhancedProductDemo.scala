package com
package mavnet
package analytics

import scala.reflect.runtime.universe.typeOf

object TestEnhancedProduct extends App {

  case class A(s: String, i: Int, d: Double)
  case class B(s2: String, d2: Double, s3: String)

  val x = A("   hello   ", 10, 1.1)
  val disassembled = x.getCaseMemberMetadata
  println(disassembled)

  val updated = disassembled.map(a => {
    a.fieldValue match {
      case value: String if value.contains("hello") => a.copy(fieldValue = value.trim + " world")
      case value: String                            => a.copy(fieldValue = value.trim)
      case value: Int                               => a.copy(fieldValue = value * 999)
      case value: Double                            => a.copy(fieldValue = value * .555)
      case _                                        => a
    }
  })
  println(updated)
  val reassembled = updated.buildCaseClassAs[A]
  println(reassembled)

  val updatedToDifferentClass = disassembled.map(a => {
    a.fieldValue match {
      case value: String => a.copy(fieldValue = value.trim)
      case value: Int    => a.copy(fieldValue = value.toDouble / 33, fieldType = typeOf[Double])
      case value: Double => a.copy(fieldValue = "world", fieldType = typeOf[String])
      case _             => a
    }
  })
  println(updatedToDifferentClass)
  val reassembledAsB = updatedToDifferentClass.buildCaseClassAs[B]
  println(reassembledAsB)

}