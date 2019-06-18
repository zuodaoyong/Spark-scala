package com.scala.spark.local.applications

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
      val sparkConf=new SparkConf().setMaster("local[*]").setAppName(Main.getClass.getSimpleName)
      val session=SparkSession.builder().config(sparkConf).getOrCreate()
      val loadDate=new LoadData
      loadDate.loadTbDate(session).show()
  }
}
