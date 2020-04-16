package com.mavnet

import org.apache.spark.sql.{SQLImplicits, SparkSession}

package object analytics extends SQLImplicits{

  lazy val spark = SparkSession.builder.appName("Mavnet_Analytics").enableHiveSupport().getOrCreate()

  override protected lazy val _sqlContext = spark.sqlContext

  implicit class StringEnhancements (s : String) {
    def increment = s.map(c => (c + 1).toChar)
    def decrement = s.map(c => (c -1 ).toChar)
    def concatWithDelimiter(delimiter : String, inStrings : String*) = inStrings.reduceLeft((currentElement, nextElement) => (currentElement.concat(delimiter).concat(nextElement)) )
  }

}