package com.scala.spark.local

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast

/**
 * 共享变量
 */
object Variable {
  val nums=Array(1,2,3,4,5,6,7,8,9)
  def main(args:Array[String]):Unit={
   val conf = new SparkConf().setAppName("Variable").setMaster("local")
   val context = new SparkContext(conf)
   //broadcastVariable(context)
   accumulator(context)
  }
  
  /**
	 * 广播变量(只读)
	 */
  def broadcastVariable(context:SparkContext){
    val f=2
    val bf=context.broadcast(f)
    context.parallelize(nums,1)
    .map(e=>e*bf.value)
    .foreach(t=>println(t))
  }
  
  /**
   * 多task共享写
   */
  def accumulator(context:SparkContext){
    val acr=context.accumulator(0)
    context.parallelize(nums,1)
           .foreach(e=>acr.add(e))
    println("acr1="+acr)
    context.parallelize(nums,1)
           .foreach(e=>acr.add(e))
    println("acr2="+acr)
  }
}