package com.scala.spark.userbehavior.mock
import scala.util.control.Breaks._
object StringUtils {

  def fulfuill(str:String):String= if (str.length == 2) return str
  else return "0" + str

  def getFieldFromConcatString(str: String, delimiter: String, field: String): String = {
    try {
      val fields = str.split(delimiter)
      for (concatField <- fields) { // searchKeywords=|clickCategoryIds=1,2,3
        if (concatField.split("=").length == 2) {
          val fieldName = concatField.split("=")(0)
          val fieldValue = concatField.split("=")(1)
          if (fieldName == field) return fieldValue
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    null
  }

  def setFieldInConcatString(str: String, delimiter: String, field: String, newFieldValue: String): String = {
     val fields=str.split(delimiter)
     for(i<-0 until fields.length){
       val fieldName=fields(i).split("=")(0)
       if(fieldName==field){
         val concatField= fieldName+"="+newFieldValue
         fields(i)=concatField
         break
       }
     }
    val buffer=new StringBuilder("")
    for(i<-0 until fields.length){
      buffer.append(fields(i))
      if(i<fields.length-1){
        buffer.append("|")
      }
    }
    buffer.toString()
  }

  /**
    * 截断字符串两侧的逗号
    * @param str
    * @return
    */
  def trimComma(str:String):String={
    var result=str
    if(result.startsWith(",")) {
      result = result.substring(1)
    }
    if(result.endsWith(",")) {
      result = result.substring(0, result.length() - 1)
    }
    result
  }
}
