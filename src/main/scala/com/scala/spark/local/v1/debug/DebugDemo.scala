package com.scala.spark.local.v1.debug

import com.scala.spark.local.v1.rdd.RDDDemo
import org.apache.spark.{SparkConf, SparkContext}

object DebugDemo {

  def main(args: Array[String]): Unit = {
    //1.本地调试
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName(RDDDemo.getClass.getSimpleName)
    val sparkContext=new SparkContext(sparkConf)
    //2.远程调试:name所在ip和本机ip在同一个网段
    val sparkConf2=new SparkConf().setMaster("spark://name:port").setAppName(RDDDemo.getClass.getSimpleName)
      .setJars(List("D:\\software\\project\\Spark-scala\\target\\Spark-scala-0.0.1-SNAPSHOT.jar"))
      .setIfMissing("spark.driver.host","本机ip")
    val sparkContext2=new SparkContext(sparkConf2)
  }
}
