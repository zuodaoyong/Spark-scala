package com.scala.spark.userbehavior.mock

import java.util.{Random, UUID}

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Row, RowFactory, SparkSession,DataFrame}

import scala.collection.mutable.ListBuffer

/**
  * 模拟数据
  */
object MockData {

  def mock(sparkContext: SparkContext,sparkSession: SparkSession): Unit ={

    val rows=ListBuffer[Row]()
    var searchKeywords=List("火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
      "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉")
    val date=DateUtils.getTodayDate()
    val actions=List("search", "click", "order", "pay")
    val random=new Random
    for(i<- 0 until 100){
      val userid=random.nextInt(100).toLong
      for(j<-0 until 10){
        val sessionid=UUID.randomUUID.toString.replace("-", "")
        val baseActionTime = date + " " + random.nextInt(23)
        for(k<- 0 until random.nextInt(100)){
          val pageid = random.nextInt(10)
          val actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)))
          var searchKeyword:String=null
          var clickCategoryId = 0L
          var clickProductId = 0L
          var orderCategoryIds:String = null
          var orderProductIds:String = null
          var payCategoryIds:String= null
          var payProductIds:String = null
          val action = actions(random.nextInt(4))
          if("search"==action) {
            searchKeyword=searchKeywords(random.nextInt(10))
          }else if ("click" == action){
            clickCategoryId = random.nextInt(100).toLong
            clickProductId = random.nextInt(100).toLong
          } else if ("order" == action) {
            orderCategoryIds = random.nextInt(100).toString
            orderProductIds = random.nextInt(100).toString
          }else if("pay"==action) {
            payCategoryIds = random.nextInt(100).toString
            payProductIds = random.nextInt(100).toString
          }

          val row=Row(date, userid, sessionid,
            pageid, actionTime, searchKeyword,
            clickCategoryId, clickProductId,
            orderCategoryIds, orderProductIds,
            payCategoryIds, payProductIds)
          rows+=row
        }
      }
    }
    val rdd1=sparkContext.parallelize(rows)
    val schema1=DataTypes.createStructType(Array(
      DataTypes.createStructField("date", DataTypes.StringType, true),
      DataTypes.createStructField("user_id", DataTypes.LongType, true),
      DataTypes.createStructField("session_id", DataTypes.StringType, true),
      DataTypes.createStructField("page_id", DataTypes.IntegerType, true),
      DataTypes.createStructField("action_time", DataTypes.StringType, true),
      DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
      DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
      DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
      DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
      DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
      DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
      DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true)
    ))
    val dataFrame1:DataFrame=sparkSession.createDataFrame(rdd1,schema1)
    dataFrame1.createOrReplaceTempView("user_visit_action")
    dataFrame1.takeAsList(1).stream().forEach(e=>println(e))

    rows.clear()

    val sexes=List("male", "female")
    for(i <-0 until 100){
      val userid = i.toLong
      val username = "user" + i
      val name = "name" + i
      val age = random.nextInt(60)
      val professional = "professional" + random.nextInt(100)
      val city = "city" + random.nextInt(100)
      val sex = sexes(random.nextInt(2))
      val row = Row(userid, username, name, age, professional, city, sex)
      rows+=row

    }
    val rdd2=sparkContext.parallelize(rows)
    val schema2=DataTypes.createStructType(Array(
      DataTypes.createStructField("user_id", DataTypes.LongType, true),
      DataTypes.createStructField("username", DataTypes.StringType, true),
      DataTypes.createStructField("name", DataTypes.StringType, true),
      DataTypes.createStructField("age", DataTypes.IntegerType, true),
      DataTypes.createStructField("professional", DataTypes.StringType, true),
      DataTypes.createStructField("city", DataTypes.StringType, true),
      DataTypes.createStructField("sex", DataTypes.StringType, true)
    ))
    val dataFrame2:DataFrame=sparkSession.createDataFrame(rdd2,schema2)
    dataFrame2.createOrReplaceTempView("user_info")
    dataFrame2.takeAsList(1).stream().forEach(e=>println(e))
  }

}