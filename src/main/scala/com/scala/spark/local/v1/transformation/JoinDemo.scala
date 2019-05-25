package com.scala.spark.local.v1.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 打印学生成绩
  */
object JoinDemo {

  def main(args:Array[String]): Unit ={
    var sparkConf=new SparkConf().setMaster("local[*]").setAppName(JoinDemo.getClass.getSimpleName)
    var sparkContext=new SparkContext(sparkConf)
    var rdd1=sparkContext.parallelize(List(Tuple2(1,"zs"),Tuple2(2,"ls"),Tuple2(3,"ww")))
    var rdd2=sparkContext.parallelize(List(Tuple2(1,90),Tuple2(2,85),Tuple2(3,100)))
    rdd1.join(rdd2).foreach(println(_))
  }
}
