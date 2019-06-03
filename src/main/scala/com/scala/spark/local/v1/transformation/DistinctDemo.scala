package com.scala.spark.local.v1.transformation

import org.apache.spark.{SparkConf, SparkContext}

object DistinctDemo {

  def main(args:Array[String]): Unit ={
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName(DistinctDemo.getClass.getSimpleName)
    val sparkContext=new SparkContext(sparkConf)
    val rdd=sparkContext.makeRDD(Array(1,2,3,2,5,7,2,4))
    val rdd2=rdd.distinct()
    rdd2.collect().foreach(println(_))
    sparkContext.stop()
  }
}
