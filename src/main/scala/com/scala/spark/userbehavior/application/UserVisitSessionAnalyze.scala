package com.scala.spark.userbehavior.application

import com.scala.spark.userbehavior.mock.MockData
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object UserVisitSessionAnalyze {

  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName("UserVisitSessionAnalyze")
      .setMaster("local[*]")
    val sparkContext=new SparkContext(sparkConf)
    val sparkSession=SparkSession.builder().getOrCreate()
    //生成测试数据
    MockData.mock(sparkContext,sparkSession)
  }
}
