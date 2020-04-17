package com
package mavnet
package analytics

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.SparkSession

object TestPad extends App {

  Logger.getRootLogger.setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  System.setProperty("hadoop.home.dir", "src/test/resources/")
  SparkSession.builder().enableHiveSupport().master("local[*]").getOrCreate()

  case class A(a: String, b: String)
  case class B(c: String, d: A)
  case class C(e: String, f: B)
  val x = Seq(C("z", B("y", A("x", "w")))).toDS
  x.flattenSchema.show

}