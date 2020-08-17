package com.scala.spark

import org.apache.log4j.lf5.LogLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.sql.SparkSession

object Test {

  def main(args: Array[String]): Unit = {
    val sparkConf =new SparkConf().setMaster("local[*]").setAppName("test")
    val sc=new SparkContext(sparkConf)
    sc.setCheckpointDir("file:///D:/");
    val rdd:RDD[Int]=sc.parallelize(List(1,2,3,4,5,6))



    val rddd=sc.textFile("");




  }

}
