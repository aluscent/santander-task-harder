package com.alitariverdy

import data.rows.Guarantee
import data.{PostgresExchangeRateRepository, PostgresReceiversRegistryRepository, PostgresTransactionRepository}
import db.SQLs

import com.alitariverdy.utils.GetDate
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.broadcast

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object SparkSqlReport extends Report {
  def main(args: Array[String]): Unit = {
    val reportDate: String = GetDate(args)

    PostgresTransactionRepository(postgresConnector, reportDate)
      .filteredTable.createOrReplaceTempView("transactions")

    broadcast(
      PostgresReceiversRegistryRepository(postgresConnector, reportDate)
        .filteredTable.cache()
    )
      .createOrReplaceTempView("receivers_registry")

    broadcast(
      PostgresExchangeRateRepository(postgresConnector, reportDate)
        .filteredTable.cache()
    )
      .createOrReplaceTempView("exchange_rates")

    println(SQLs.sqlReport(reportDate))

    import sparkSession.implicits._
    val guarantees: Dataset[Guarantee] =
      sparkSession.sql(SQLs.sqlReport(reportDate)).as[Guarantee]

    postgresConnector.writeTable(guarantees, """"public"."guarantees"""")
  }
}
