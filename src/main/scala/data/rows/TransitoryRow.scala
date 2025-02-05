package com.alitariverdy
package data.rows

case class TransitoryRow(
                          country: String,
                          currency: String,
                          txn_type: String,
                          partition_date: String,
                          amount: BigDecimal,
                          guarantor_name: String
)
