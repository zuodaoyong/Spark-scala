package com.scala.spark.local.v1.transformation

import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName(CombineByKeyDemo.getClass.getSimpleName)
      .setMaster("local[*]")
    val sparkContext=new SparkContext(sparkConf)
    val rdd=sparkContext.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,8)),3);
    rdd.aggregateByKey(0)(Math.max(_,_),_+_).foreach(println(_))

  }

}
