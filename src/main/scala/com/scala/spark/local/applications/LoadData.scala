package com.scala.spark.local.applications

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

case class tbStock(orderno:String,locationid:String,date:String)
case class tbStockDetail(orderno:String,rownum:Int,itemid:String,num:Int,price:Double,amount:Double) extends Serializable
case class tbDate(date:String,years:Int,thisyear:Int,month:Int,day:Int,weekday:Int,week:Int
                 ,quarter:Int,period:Int,halfmonth:Int) extends Serializable
class LoadData {

  /**
    * 加载tbStock数据
    * @param session
    * @return
    */
  def loadTbStock(session:SparkSession)={
     val context=session.sparkContext
     val rdd=context.textFile("src\\main\\resources\\sql\\tbStock.txt")
     val tbStockRDD=rdd.map(line=>line.split(",")).map(arr=>tbStock(arr(0).toString,arr(1).toString,arr(2).toString));
     import session.implicits._
     val tbStockDS=tbStockRDD.toDS()
    tbStockDS.createOrReplaceTempView("tbStock")
  }

  /**
    * 加载tbStockDetail数据
    * @param session
    * @return
    */
  def loadTbStockDetail(session:SparkSession)={
    val context=session.sparkContext
    val rdd=context.textFile("src\\main\\resources\\sql\\tbStockDetail.txt")
    val tbStockDetailRDD=rdd.map(line=>line.split(","))
      .map(arr=>tbStockDetail(arr(0).toString,arr(1).toInt,arr(2).toString,
        arr(3).toInt,arr(4).toDouble,arr(5).toDouble));
    import session.implicits._
    val tbStockDetailDS=tbStockDetailRDD.toDS()
    tbStockDetailDS.createOrReplaceTempView("tbStockDetail")
  }

  /**
    * 加载tbDate数据
    * @param session
    * @return
    */
  def loadTbDate(session:SparkSession)={
    val context=session.sparkContext
    val rdd=context.textFile("src\\main\\resources\\sql\\tbDate.txt")
    val tbDateRDD=rdd.map(line=>line.split(","))
      .map(arr=>tbDate(arr(0).toString,arr(1).toInt,arr(2).toInt,
        arr(3).toInt,arr(4).toInt,arr(5).toInt,arr(6).toInt,arr(7).toInt,arr(8).toInt,arr(9).toInt));
    import session.implicits._
    val tbDateDS=tbDateRDD.toDS()
    tbDateDS.createOrReplaceTempView("tbDate")
  }
}
