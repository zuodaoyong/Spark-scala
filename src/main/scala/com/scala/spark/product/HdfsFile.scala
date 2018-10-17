package com.scala.spark.local

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object HdfsFile {
  
  def main(args:Array[String]):Unit={
     val conf=new SparkConf().setAppName("localFile");  
     val context=new SparkContext(conf);
     val rdd=context.textFile("hdfs://home0:9000/test/letter",2);
     val len=rdd.map(e=>e.length()).reduce(_+_)
     println(len)
  }
}