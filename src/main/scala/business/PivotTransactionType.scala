package com.alitariverdy
package business

import data.rows.{Guarantee, TransitoryRow}

import org.apache.spark.sql.functions.{avg, col, lit, sum}
import org.apache.spark.sql.{Dataset, Encoder}

object PivotTransactionType {
  def apply(
       reportDate: String,
      df: Dataset[TransitoryRow]
  )(implicit eg: Encoder[Guarantee], es: Encoder[TransitoryRow]): Dataset[Guarantee] =
    df.map(txn => txn.copy(txn_type = txn.txn_type.toLowerCase))
      .groupBy(col("guarantor_name"), col("country"), lit(reportDate).alias("partition_date"))
      .pivot("txn_type", Seq("class_a", "class_b", "class_c", "class_d"))
      .agg(sum(col("amount")).as("sum"), avg(col("amount")).as("avg"))
      .na
      .fill(0)
      .as[Guarantee]
}
