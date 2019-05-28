package com.scala.spark.local.v1.transformation

import org.apache.spark.{SparkConf, SparkContext}

object SampleDemo {

  def main(args:Array[String]): Unit ={
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(SampleDemo.getClass.getSimpleName)
    val sparkContext=new SparkContext(sparkConf)
    val rdd=sparkContext.makeRDD(1 to 100)
    rdd.sample(false,0.2,10).foreach(println(_))
  }
}
