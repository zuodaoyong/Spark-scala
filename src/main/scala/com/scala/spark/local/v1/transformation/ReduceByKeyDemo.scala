package com.scala.spark.local.v1.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计每个班级的总分(不需要Shuffer)
  */
object ReduceByKeyDemo {

  def main(args:Array[String]): Unit ={
    var sparkConf = new SparkConf().setMaster("local[*]").setAppName(ReduceByKeyDemo.getClass.getSimpleName)
    var sparkContext=new SparkContext(sparkConf)
    var rdd=sparkContext.parallelize(List(Tuple2("class1",90),Tuple2("class2",95),Tuple2("class1",80)))
    rdd.reduceByKey(_+_,2).foreach(println(_))
  }
}
