package com
package mavnet
package analytics

object TestEnhancedDataset extends App with SparkSessionLoader {

  case class A(a: String, b: String)
  case class B(c: String, d: A)
  case class C(e: String, f: B)
  val x = Seq(C("z", B("y", A("x", "w")))).toDS
  x.flattenSchema.show

}