package com.alitariverdy
package data.rows

case class Guarantee(
    guarantor_name: String,
    country: String,
    partition_date: String,
    class_a_sum: BigDecimal,
    class_a_avg: BigDecimal,
    class_b_sum: BigDecimal,
    class_b_avg: BigDecimal,
    class_c_sum: BigDecimal,
    class_c_avg: BigDecimal,
    class_d_sum: BigDecimal,
    class_d_avg: BigDecimal
)
