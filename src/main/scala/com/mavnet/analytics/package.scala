package com.mavnet

import org.apache.spark.sql.{ Column, Dataset, DataFrame, SQLImplicits, SparkSession }
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ StructField, StructType }
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe.{ MethodSymbol, TermName, Type, TypeTag, typeOf }

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
          case Nil => header
        }
      }
      ds.select(traverseSchema(ds.schema.map(_ -> "")): _*)
    }

  }

  implicit class EnhancedProduct[A <: Product: TypeTag](cc: A) {

    case class FieldMetadata(fieldName: String, fieldType: Type, fieldValue: Any, fieldArrity: Int)

    case class DisassembledCaseClass(private val fieldMetadata: Seq[FieldMetadata]) {

      def buildCaseClassAs[B <: Product: TypeTag]: B = {
        val companionObjectSymbol = currentMirror.symbolOf[B].companion.asModule
        val instanceToReflect = currentMirror.reflectModule(companionObjectSymbol).instance
        val instanceMirror = currentMirror.reflect(instanceToReflect)
        val methodToReflect = TermName("apply")
        val instanceTypeSignature = instanceMirror.symbol.typeSignature
        val methodSymbol = instanceTypeSignature.member(methodToReflect).asMethod
        val apply = instanceMirror.reflectMethod(methodSymbol)
        apply(this.fieldMetadata.map(_.fieldValue): _*).asInstanceOf[B]
      }

      def map(f: FieldMetadata => FieldMetadata): DisassembledCaseClass = {
        DisassembledCaseClass(this.fieldMetadata.map(f))
      }

      override def toString = this.fieldMetadata.toString

    }

    def getCaseMemberMetadata: DisassembledCaseClass = {
      val metadata = typeOf[A].members.sorted.collect {
        case m: MethodSymbol if m.isCaseAccessor => (m.returnType, m.name.toString)
      }.zip(cc.productIterator.toSeq).zipWithIndex.map(a => FieldMetadata(a._1._1._2, a._1._1._1, a._1._2, a._2))
      DisassembledCaseClass(metadata)
    }

  }

}