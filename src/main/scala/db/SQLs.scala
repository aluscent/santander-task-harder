package com.alitariverdy
package db

object SQLs {
  val sqlReport: String => String = (reportDate: String) => s"""
      |WITH LatestExchangeRates AS (
      |  SELECT from_currency, rate
      |  FROM exchange_rates
      |  WHERE partition_date <= '$reportDate'
      |),
      |ValidTransactions AS (
      |  SELECT DISTINCT t.*
      |  FROM transactions t
      |  WHERE EXISTS (
      |      SELECT 1 FROM receivers_registry rr
      |      WHERE t.receiver_name = rr.receiver_account
      |        AND t.country = rr.country
      |        AND rr.partition_date <= '$reportDate'
      |    )
      |  AND t.partition_date <= '$reportDate'
      |),
      |ExplodedGuarantors AS (
      |  SELECT
      |    vt.*,
      |    guarantor.NAME AS guarantor_name,
      |    CAST(guarantor.PERCENTAGE AS DECIMAL(5,2)) AS guarantor_percentage
      |  FROM ValidTransactions vt
      |  LATERAL VIEW explode(
      |    from_json(vt.guarantors, 'array<map<string,string>>')
      |  ) g AS guarantor
      |)
      |SELECT
      |  CAST(guarantor_name AS VARCHAR(50)) AS guarantor_name,
      |  CAST(country AS VARCHAR(50)) AS country,
      |  '$reportDate' AS partition_date,
      |  SUM(NVL(class_a_sum, 0)) AS class_a_sum,
      |  SUM(NVL(class_a_avg, 0)) AS class_a_avg,
      |  SUM(NVL(class_b_sum, 0)) AS class_b_sum,
      |  SUM(NVL(class_b_avg, 0)) AS class_b_avg,
      |  SUM(NVL(class_c_sum, 0)) AS class_c_sum,
      |  SUM(NVL(class_c_avg, 0)) AS class_c_avg,
      |  SUM(NVL(class_d_sum, 0)) AS class_d_sum,
      |  SUM(NVL(class_d_avg, 0)) AS class_d_avg
      |FROM ExplodedGuarantors eg
      |JOIN LatestExchangeRates ler ON eg.currency = ler.from_currency
      | PIVOT (
      |   sum(amount * rate * guarantor_percentage) as sum,
      |   avg(amount * rate * guarantor_percentage) as avg
      |   FOR txn_type IN ('CLASS_A' as class_a, 'CLASS_B' as class_b, 'CLASS_C' as class_c, 'CLASS_D' as class_d)
      | )
      |GROUP BY guarantor_name, country, '$reportDate'
      """.stripMargin
}
