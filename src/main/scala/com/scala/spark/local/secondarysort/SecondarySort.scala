package com.scala.spark.local.secondarysort

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SecondarySort {
  
  def main(args:Array[String]):Unit={
    val conf=new SparkConf().setAppName("SecondarySort").setMaster("local")
    val context=new SparkContext(conf);
    context.textFile("src\\main\\resources\\sort.txt",1)
           .map(line=>{
             val arr=line.split("\\s")
             ((arr(0),arr(1)),line)
           })
           .sortByKey()
           .map(t=>t._2)
           .foreach(t=>println(t))
  }
}