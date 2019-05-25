package com.scala.spark.local.v1.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 过滤出集合中的偶数
  */
object FilterDemo {

  def main(args:Array[String]): Unit ={
    var sparkConf=new SparkConf().setMaster("local[*]").setAppName(FilterDemo.getClass.getSimpleName)
    var sparkContext=new SparkContext(sparkConf)
    var rdd=sparkContext.parallelize(List(1,2,3,4))
    rdd.filter(_%2==0).foreach(println(_))
  }
}
