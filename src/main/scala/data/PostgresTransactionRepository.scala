package com.alitariverdy
package data

import data.rows.Transaction
import db.PostgresConnector

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

class PostgresTransactionRepository(val postgresConnector: PostgresConnector, reportDate: String)(implicit
    e: Encoder[Transaction]
) extends PostgresRepository[Transaction]("transactions") {

  override lazy val filteredTable: Dataset[Transaction] = table
    .filter(
      col("initiation_date") >= add_months(date_trunc("month", lit(reportDate)), -1)
        and col("status") === lit("OPEN")
    )
    .dropDuplicates(List("transaction_id"))
}

object PostgresTransactionRepository {
  def apply(
      postgresConnector: PostgresConnector, reportDate: String
  )(implicit ss: SparkSession): PostgresTransactionRepository = {
    import ss.implicits._
    new PostgresTransactionRepository(postgresConnector, reportDate)
  }
}
