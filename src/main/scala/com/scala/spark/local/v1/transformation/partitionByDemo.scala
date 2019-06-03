package com.scala.spark.local.v1.transformation

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object partitionByDemo {

  def main(args:Array[String]): Unit ={
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(partitionByDemo.getClass.getSimpleName)
    val sparkContext=new SparkContext(sparkConf)
    val rdd=sparkContext.makeRDD(List((1,"a"),(2,"b")))
    var size=rdd.partitions.size
    println("size="+size)
    rdd.partitionBy(new HashPartitioner(3)).foreach(println(_))
  }
}
