package com.scala.spark.local

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SortWordCount {
  
  def main(args:Array[String]):Unit={
      val conf=new SparkConf().setAppName("SortWordCount").setMaster("local")
      val context=new SparkContext(conf)
      context.textFile("D:\\software\\temp\\spark\\wordcount", 1)
             .flatMap(line=>line.split("\\s"))
             .map(word=>(word,1))
             .reduceByKey(_+_)
             .map(t=>(t._2,t._1))
             .sortByKey(false,1)
             .map(t=>(t._2,t._1))
             .foreach(t=>println(t))
  }
}