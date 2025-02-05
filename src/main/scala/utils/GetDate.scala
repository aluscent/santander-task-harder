package com.alitariverdy
package utils

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object GetDate {
  def apply(args: Array[String]): String =
    if (!args.isEmpty) args(0)
    else LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
}
