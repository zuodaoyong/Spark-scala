package com.scala.spark.local.v1.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * rdd的创建有三种方式：
  * 1、集合创建,parallelize,makeRDD
  * 2、从外部存储来创建
  * 3、从其他rdd转换来
  */
object RDDDemo {

  def main(args:Array[String]): Unit ={
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName(RDDDemo.getClass.getSimpleName)
    val sparkContext=new SparkContext(sparkConf)
    createRDD1(sparkContext)
  }

  def createRDD1(sparkContext:SparkContext): Unit ={
    val rdd=sparkContext.parallelize(1 to 10)
    //rdd.collect().foreach(println(_))
    val rdd2=sparkContext.makeRDD(List(1,2,3,4),3)
    //rdd2.foreach(println(_))
    val rdd3=sparkContext.makeRDD(List((1,List("a","b","c")),(2,List("d","e","f"))))
    //rdd3.foreach(println(_))
    rdd3.preferredLocations(rdd3.partitions(1)).foreach(println(_))
  }


}
