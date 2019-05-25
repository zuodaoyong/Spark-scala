package com.scala.spark.local.v1.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 按照学生成绩进行排序
  */
object SortByKeyDemo {

  def main(args:Array[String]): Unit ={
    var sparkConf = new SparkConf().setMaster("local[*]").setAppName(SortByKeyDemo.getClass.getSimpleName)
    var sparkConext=new SparkContext(sparkConf)
    //var rdd=sparkConext.parallelize(List(Tuple2(90,"zs"),Tuple2(100,"ls"),Tuple2(60,"ww")))
    var rdd=sparkConext.parallelize(List(Tuple2("zs",90),Tuple2("ls",100),Tuple2("ww",60)),1)
    rdd.map(f=>Tuple2(f._2,f._1)).sortByKey(false).foreach(println(_))
  }
}
