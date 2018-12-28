package com.scala.spark.local.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object WordCountStreaming {
  
  def main(args:Array[String]):Unit={
    val conf=new SparkConf().setAppName("WordCountStreaming").setMaster("local[2]")
    val context=new SparkContext(conf)
    val streamContext=new StreamingContext(context,Seconds(5))
    
    val ds1=streamContext.socketTextStream("192.168.1.110",8888)
    val ds2=ds1.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    ds2.print()
    streamContext.start()
    streamContext.awaitTermination()
  }
}