package com.alitariverdy

import db.PostgresConnector

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

trait Report {
  lazy val config: Config = ConfigFactory.load()

  implicit lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  lazy val postgresConnector: PostgresConnector =
    PostgresConnector(config.getConfig("app.postgres"))
}
