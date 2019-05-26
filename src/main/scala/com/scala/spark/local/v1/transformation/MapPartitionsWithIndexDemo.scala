package com.scala.spark.local.v1.transformation

import com.scala.spark.local.v1.transformation.MapPartitionsDemo.fun
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsWithIndexDemo {

  def main(args:Array[String]): Unit ={
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName(MapPartitionsDemo.getClass.getSimpleName)
    val sparkContext=new SparkContext(sparkConf)
    val rdd=sparkContext.makeRDD(List(("a","female"),("b","male"),("c","female"),("d","female"),("e","female")))

    println("==============="+rdd.partitions.length)
    rdd.mapPartitionsWithIndex(fun).collect().foreach(println(_))
  }

  def fun(index:Int,iterator: Iterator[(String,String)]):Iterator[String]={
      var woman=List[String]()
      while (iterator.hasNext){
        val next=iterator.next()
        next match {
            case (name,"female")=>woman=index.toString+name::woman
            case _=>
        }
      }
    woman.iterator
  }
}
