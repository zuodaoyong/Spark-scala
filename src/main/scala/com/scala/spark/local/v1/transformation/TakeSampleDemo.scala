package com.scala.spark.local.v1.transformation

import org.apache.spark.{SparkConf, SparkContext}

object TakeSampleDemo {
  def main(args:Array[String]): Unit ={
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName(TakeSampleDemo.getClass.getSimpleName)
    val sparkContext=new SparkContext(sparkConf)
    val rdd=sparkContext.makeRDD(1 to 100)
    rdd.takeSample(false,2,10).foreach(println(_))
    sparkContext.stop()
  }
}
