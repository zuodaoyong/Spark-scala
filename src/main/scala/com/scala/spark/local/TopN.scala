package com.scala.spark.local

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * topN
 */
object TopN {
  
  
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("TopN").setMaster("local")
    val context=new SparkContext(conf)
    context.textFile("src\\main\\resources\\top.txt", 1)
           .map(line=>(line.toInt,line))
           .sortByKey(false)
           .map(t=>t._1)
           .take(3)
           .foreach(t=>println(t))
   
  }
}