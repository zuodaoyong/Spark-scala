package com.scala.spark.userbehavior.mock

object StringUtils {

  def fulfuill(str:String):String= if (str.length == 2) return str
  else return "0" + str
}
