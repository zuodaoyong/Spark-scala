package com.scala.spark.userbehavior.mock

import java.text.SimpleDateFormat
import java.util.Date

object DateUtils {

  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")

  def getTodayDate(): String = DATE_FORMAT.format(new Date)
}
