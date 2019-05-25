package com.scala.spark.local.v1.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将每个班级的成绩进行分组
  */
object GroupByKeyDemo {

  def main(args:Array[String]): Unit ={
    var sparkConf=new SparkConf().setMaster("local[*]").setAppName(GroupByKeyDemo.getClass.getSimpleName)
    var sparkContext=new SparkContext(sparkConf)
    var rdd=sparkContext.parallelize(List(Tuple2("class1",90),Tuple2("class2",95),Tuple2("class1",80)))
    rdd.groupByKey(1).foreach(println(_))
  }
}
