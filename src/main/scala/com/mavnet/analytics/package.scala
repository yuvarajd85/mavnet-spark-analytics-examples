package com.mavnet

import org.apache.spark.sql.{ SQLImplicits, SparkSession }
import org.apache.spark.sql.{ Column, Dataset, DataFrame }
import org.apache.spark.sql.types.{ StructField, StructType }
import org.apache.spark.sql.functions.col
import scala.reflect.runtime.universe.TypeTag

package object analytics extends SQLImplicits {

  lazy val spark = SparkSession.builder.appName("Mavnet_Analytics").enableHiveSupport().getOrCreate()

  override protected lazy val _sqlContext = spark.sqlContext

  implicit class EnhancedString(s: String) {

    def increment = s.map(c => (c + 1).toChar)
    def decrement = s.map(c => (c - 1).toChar)

  }

  implicit class EnhancedDataset[A <: Product: TypeTag](ds: Dataset[A]) {

    def flattenSchema: DataFrame = {
      @scala.annotation.tailrec
      def traverseSchema(fields: Seq[(StructField, String)], header: Seq[Column] = Seq.empty): Seq[Column] = {
        fields match {
          case (field, prefix) :: tail => {
            field.dataType match {
              case st: StructType => traverseSchema(st.map(_ -> s"${prefix}${field.name}.") ++ tail, header)
              case _              => traverseSchema(tail, header :+ col(s"${prefix}${field.name}"))
            }
          }
          case nil => header
        }
      }
      ds.select(traverseSchema(ds.schema.map(_ -> "")): _*)
    }

  }

}