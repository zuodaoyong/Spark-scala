package com.scala.spark.local.v1.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将集合中的数字乘以2
  */
object MapDemo {

  def main(args:Array[String]): Unit ={

    var sparkConf=new SparkConf().setMaster("local[*]").setAppName(MapDemo.getClass.getSimpleName)
    var sparkContext=new SparkContext(sparkConf)
    var rdd=sparkContext.parallelize(Array(1,2,3,4),2)
    rdd.map(_*2).foreach(println(_))
  }
}
