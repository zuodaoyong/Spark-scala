package com.scala.spark.local.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object LoadAndSave {

  def main(args:Array[String]): Unit ={
    val sparkConf=new SparkConf().setMaster("local[*]")
      .setAppName(LoadAndSave.getClass.getSimpleName)
    val sparkContext=new SparkContext(sparkConf)
    val sqlContext=new SQLContext(sparkContext)
    import sqlContext.implicits._
    //val peopleDF=sqlContext.read.json("src\\main\\resources\\sql\\people.json")
    val peopleDF=sqlContext.read.format("json").load("src\\main\\resources\\sql\\people.json")
    //peopleDF.show(1,true)
    val textDF=sqlContext.read.format("text").load("D:\\software\\temp\\spark\\stu.txt")
    textDF.show()
  }
}
