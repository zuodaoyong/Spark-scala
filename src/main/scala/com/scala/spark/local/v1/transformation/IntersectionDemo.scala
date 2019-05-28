package com.scala.spark.local.v1.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 交集
  */
object IntersectionDemo {
   def main(args:Array[String]): Unit ={
     val sparkConf =new SparkConf().setMaster("local[*]").setAppName(IntersectionDemo.getClass.getSimpleName)
     val sparkContext=new SparkContext(sparkConf)
     val rdd1=sparkContext.makeRDD(1 to 10)
     val rdd2=sparkContext.makeRDD(8 to 15)
     val rdd3=rdd1.intersection(rdd2)
     rdd3.foreach(println(_))
     sparkContext.stop()
   }
}
