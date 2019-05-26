package com.scala.spark.local.v1.transformation

import org.apache.spark.{SparkConf, SparkContext}


object MapPartitionsDemo {

  def main(args:Array[String]): Unit ={
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName(MapPartitionsDemo.getClass.getSimpleName)
    val sparkContext=new SparkContext(sparkConf)
    val rdd=sparkContext.makeRDD(List(("a","female"),("b","male"),("c","female")))
    rdd.mapPartitions(fun).collect().foreach(println(_))
  }

  def fun(iterator: Iterator[(String,String)]):Iterator[String]={
    var woman=List[String]()
    while (iterator.hasNext){
      val next=iterator.next()
      next match {
          case (_,"female")=>woman=next._1::woman
          case _=>
      }
    }
    woman.iterator
  }
}
