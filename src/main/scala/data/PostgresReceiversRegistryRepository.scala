package com.alitariverdy
package data

import db.PostgresConnector
import data.rows.ReceiversRegistry

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{col, lit, max}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

class PostgresReceiversRegistryRepository(val postgresConnector: PostgresConnector, reportDate: String)(implicit
    ss: SparkSession,
    err: Encoder[ReceiversRegistry],
    es: Encoder[String]
) extends PostgresRepository[ReceiversRegistry]("receivers_registry") {

  private val latestReceiverRegistryDate: Broadcast[String] = ss.sparkContext.broadcast(
    table
      .filter(col("partition_date") <= lit(reportDate))
      .select(max(col("partition_date")))
      .as[String]
      .collect()
      .head
  )

  override lazy val filteredTable: Dataset[ReceiversRegistry] = table
    .filter(t => t.partition_date == latestReceiverRegistryDate.value)
    .dropDuplicates(List("receiver_account"))
}

object PostgresReceiversRegistryRepository {
  def apply(
      postgresConnector: PostgresConnector, reportDate: String
  )(implicit ss: SparkSession): PostgresReceiversRegistryRepository = {
    import ss.implicits._
    new PostgresReceiversRegistryRepository(postgresConnector, reportDate)
  }
}
