package com.alitariverdy
package data.rows

import data.TableRow

case class ExchangeRate(
    from_currency: String,
    to_currency: String,
    rate: BigDecimal,
    partition_date: String
) extends TableRow
