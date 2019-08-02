package com.test

import org.apache.spark.{SparkConf, SparkContext}


object Test {

  def main(args: Array[String]): Unit = {
      val sparkConf= new SparkConf().setAppName(Test.getClass.getSimpleName).setMaster("local[*]")
      val sparkContext=new SparkContext(sparkConf)
      val rdd=sparkContext.textFile("hdfs://10.100.191.156:8020/test/flow.txt")
  }



}

