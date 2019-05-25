package com.scala.spark.local.v1.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将一行文本转换成单词
  */
object FlatMapDemo {
    def main(args:Array[String]): Unit ={
      var sparkConf =new SparkConf().setMaster("local[*]").setAppName(FlatMapDemo.getClass.getSimpleName)
      var sparkContext=new SparkContext(sparkConf)
      var rdd=sparkContext.parallelize(List("I am very naughty","she smiles at first and then turns angry"))
      rdd.flatMap(_.split("\\s")).foreach(println(_))
    }
}
