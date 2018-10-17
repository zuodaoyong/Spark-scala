package com.scala.spark.local

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object LocalFile {
  
  def main(args:Array[String]):Unit={
     val conf=new SparkConf().setAppName("localFile").setMaster("local");  
     val context=new SparkContext(conf);
     val rdd=context.textFile("D:\\software\\temp\\spark\\letter",2);
     val len=rdd.map(e=>e.length()).reduce(_+_)
     println(len)
  }
}