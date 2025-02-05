package com.alitariverdy
package business

import data.PostgresExchangeRateRepository
import data.rows.Transaction
import db.PostgresConnector

import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

object EuroExchangeRates {
  def apply(df: Dataset[Transaction], postgresConnector: PostgresConnector, reportDate: String)(
      implicit ss: SparkSession
  ): Dataset[Transaction] = {

    import ss.implicits._

    val exchangeRates =
      broadcast(PostgresExchangeRateRepository(postgresConnector, reportDate).filteredTable.cache())

    df.joinWith(exchangeRates, df.col("currency") === exchangeRates.col("from_currency"))
      .map { row =>
        val exchangeAmount = row._1.amount * row._2.rate
        row._1.copy(amount = exchangeAmount)
      }
  }
}
