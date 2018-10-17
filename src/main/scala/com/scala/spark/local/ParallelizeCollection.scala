package com.scala.spark.local

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ParallelizeCollection {
  
  def main(args:Array[String]):Unit={
      val arr=1 to 10;
      val conf=new SparkConf().setAppName("parallelizeCollection")
                     .setMaster("local");
      val context=new SparkContext(conf);
      val result=context.parallelize(arr).reduce((e1,e2)=>(e1+e2));
      println(result)
  }
}