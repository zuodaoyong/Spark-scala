package com.scala.spark.local.rdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object aggregateTest {
  
  def main(args:Array[String]){
   
    val conf=new SparkConf().setAppName("aggregateTest").setMaster("local")
    val context=new SparkContext(conf)
    //aggregate(context)
    //aggregateByKey(context)
    cartesian(context)
  }
  
  /**
   * 第一个rdd和第二个rdd的每一个元素join
   */
  def cartesian(context:SparkContext){
    val x = context.parallelize(List(1,2,3,4,5))
    val y = context.parallelize(List(6,7,8,9,10))
    val rdd=x.cartesian(y)
    rdd.map(e=>println(e._1+":"+e._2)).collect()
  }
  
  def aggregateByKey(context:SparkContext){
    val pairRDD = context.parallelize(List( ("cat",2), ("cat", 5), ("mouse", 4),("cat", 12), ("dog", 12), ("mouse", 2)), 2)
    val aa=pairRDD.mapPartitionsWithIndex(funcTuple).collect()
    val result=pairRDD.aggregateByKey(0)(_+_,_+_)
    result.map(e=>println(e)).collect()
    
  }
  
  /**
   * 聚合
   */
  def aggregate(context:SparkContext){
    var rdd1=context.parallelize(List(1,2,3,4,5,6),2)
    var rdd2=rdd1.mapPartitionsWithIndex(funcInt).collect()
    var rdd3=rdd1.aggregate(1)(_+_,_+_)
  }
  
  def funcInt(index:Int,iter:Iterator[Int]):Iterator[Int]={
    iter.map(e=>{
      println("["+index+":"+e+"]")
      e
    })
  }
  
  def funcTuple(index:Int,iter:Iterator[(String,Int)]):Iterator[(String,Int)]={
    iter.map(e=>{
         println("["+index+":("+e._1+","+e._2+")]")
         e
      })
  }
  
}