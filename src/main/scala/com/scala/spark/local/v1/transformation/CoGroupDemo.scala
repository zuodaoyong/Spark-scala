package com.scala.spark.local.v1.transformation

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

/**
  * 打印学生成绩
  */
object CoGroupDemo {

  def main(args:Array[String]): Unit ={
    var sparkConf=new SparkConf().setMaster("local[*]").setAppName(CoGroupDemo.getClass.getSimpleName)
    var sparkContext=new JavaSparkContext(sparkConf)
    var rdd1=sparkContext.parallelize(List(Tuple2(1,"zs"),Tuple2(2,"ls"),Tuple2(3,"ww")))
    var rdd2=sparkContext.parallelize(List(Tuple2(1,90),Tuple2(2,85),Tuple2(3,100)))
    rdd1.cogroup(rdd2).foreach(println(_))
  }
}
