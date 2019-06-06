package com.scala.spark.local.v1.rdd

import org.apache.spark.{SparkConf, SparkContext}

object CheckPointerDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName(CheckPointerDemo.getClass.getSimpleName)
    val sparkContext=new SparkContext(sparkConf)
    sparkContext.setCheckpointDir("D:\\software\\temp\\spark\\check")
    val check = sparkContext.makeRDD(1 to 10)
    val check2=check.map(_.toString+"["+System.currentTimeMillis()+"]")
    val nocheck=check.map(_.toString+"["+System.currentTimeMillis()+"]")
    check2.checkpoint()
    println("1.===")
    check2.collect().foreach(println(_))
    println("2.===")
    check2.collect().foreach(println(_))
    println("3.===")
    check2.collect().foreach(println(_))
    println("no1.===")
    nocheck.collect().foreach(println(_))
    println("no2.===")
    nocheck.collect().foreach(println(_))
    println("no3.===")
    nocheck.collect().foreach(println(_))
  }
}


