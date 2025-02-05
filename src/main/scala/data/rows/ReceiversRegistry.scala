package com.alitariverdy
package data.rows

import data.TableRow

case class ReceiversRegistry(
    receiver_account: String,
    country: String,
    partition_date: String
) extends TableRow
