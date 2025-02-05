package com.alitariverdy
package data

import db.PostgresConnector

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Encoder}

import scala.reflect.runtime.universe.TypeTag

abstract class PostgresRepository[T <: TableRow : TypeTag](val tableName: String)
                                                          (implicit et: Encoder[T])
  extends Serializable {

  val postgresConnector: PostgresConnector

  val catalog = "public"
  val tableFQN = s""""$catalog"."$tableName""""

  val schema: StructType =
    ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

  val table: Dataset[T] = postgresConnector
    .readTable(tableFQN, schema)
    .as[T]

  val filteredTable: Dataset[T]
}
