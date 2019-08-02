package com.scala.spark.product

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object StandAlone {

  def main(args: Array[String]): Unit = {
     val sparkConf=new SparkConf().setMaster("local").setAppName(StandAlone.getClass.getSimpleName)
     val sparkContext=new SparkContext(sparkConf)
     val broadcast=sparkContext.broadcast(List("a","b"))
     val rdd=sparkContext.parallelize(List("hello java","hello scala","hello pythton"))
     rdd.flatMap(line=>line.split(" ")).mapPartitions(fun1).foreachPartition((iterator)=>{
          while (iterator.hasNext){
            val word=iterator.next()
            println(broadcast.value)
          }
     })

  }

  def fun1(iterator: Iterator[String]): Iterator[String] ={
    val list=ListBuffer[String]()
    while (iterator.hasNext){
      val line=iterator.next()
      val arr=line.split(" ")
      for(i<-0 until arr.length){
        list.append(arr(i))
      }
    }
    list.iterator
  }
}
