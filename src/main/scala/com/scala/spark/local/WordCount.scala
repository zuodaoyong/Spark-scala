package com.scala.spark.local

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args:Array[String]):Unit={
    val conf=new SparkConf().setAppName("wordcount").setMaster("local")
    val context=new SparkContext(conf)
    val lines=context.textFile("D:\\software\\temp\\spark\\wordcount")
    val wordCounts=lines.flatMap(line=>line.split("\\s"))
         .map(word=>(word,1))
         .reduceByKey(_+_)
     wordCounts.foreach(wordcount=>print(wordcount._1+":"+wordcount._2))
  }
}