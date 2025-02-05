package com.alitariverdy
package db

import data.rows.Guarantee

import com.typesafe.config.Config
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

class PostgresConnector(postgresConfig: Config)(implicit sparkSession: SparkSession)
    extends Serializable {

  private val connectionProperties: Broadcast[Map[String, String]] =
    sparkSession.sparkContext.broadcast(
      Map(
        "user" -> sys.env("POSTGRES_USER"),
        "password" -> sys.env("POSTGRES_PASSWORD"),
        "driver" -> postgresConfig.getString("driver"),
        "url" -> postgresConfig.getString("url"),
        "checkpointLocation" -> postgresConfig.getString("checkpointLocation")
      )
    )

  def readTable(tableFQN: String, schema: StructType): DataFrame = sparkSession.read
    .format("jdbc")
//    .schema(schema)
    .option("dbtable", tableFQN)
    .option("pushDownPredicate", "true")
    .options(connectionProperties.value)
    .load()

  def writeTable(df: Dataset[Guarantee], tableFQN: String): Unit = df.write
    .format("jdbc")
    .option("dbtable", tableFQN)
    .options(connectionProperties.value)
    .mode(SaveMode.Append)
    .partitionBy("partition_date")
    .save()
}

object PostgresConnector {

  /** Wraps methods related to Postgres for ease of use.
    * @param postgresConfig config loaded from "app.postgres"
    * @param sparkSession the Spark session used by main app
    * @return an object of type [[PostgresConnector]]
    */
  def apply(postgresConfig: Config)(implicit sparkSession: SparkSession) =
    new PostgresConnector(postgresConfig)
}
