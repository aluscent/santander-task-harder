package com.alitariverdy
package business

import db.PostgresConnector
import data.PostgresReceiversRegistryRepository
import data.rows.Transaction

import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

object CheckRegisteredReceivers {
  def apply(df: Dataset[Transaction], postgresConnector: PostgresConnector, reportDate: String)(implicit
      ss: SparkSession
  ): Dataset[Transaction] = {

    import ss.implicits._

    val receiversRegistry =
      broadcast(PostgresReceiversRegistryRepository(postgresConnector, reportDate).filteredTable.cache())

    df.joinWith(
      receiversRegistry,
      df.col("receiver_name") === receiversRegistry.col("receiver_account")
        and df.col("country") === receiversRegistry.col("country")
    ).map(_._1)
  }
}
