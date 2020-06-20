package com.scala.spark.local.v1.transformation

import org.apache.spark.{SparkConf, SparkContext}

object TreeReduceDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf =new SparkConf().setMaster("local[*]")
      .setAppName(TreeReduceDemo.getClass.getSimpleName)
    val sc=new SparkContext(sparkConf)
    val rdd=sc.parallelize(List(-5,-4,-3,-2,-1,1,2,3,4),10)
    val result=rdd.treeReduce((x,y)=>x+y,5)
    println(result)
  }
}
