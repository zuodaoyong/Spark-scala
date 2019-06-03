package com.scala.spark.local.v1.transformation

import org.apache.spark.{SparkConf, SparkContext}

object CombineByKeyDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName(CombineByKeyDemo.getClass.getSimpleName)
      .setMaster("local[*]")
    val sparkContext=new SparkContext(sparkConf)
    var rdd=sparkContext.parallelize(List(Tuple2("class1",90),Tuple2("class2",95),Tuple2("class1",80),Tuple2("class1",82)))
    var combine=rdd.combineByKey((v)=>(v,1),(acc:(Int,Int),v)=>(acc._1+v,acc._2+1),(acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2))
    combine.collect().foreach(println(_))
    combine.map{case (k,v)=>(k,v._1/v._2.toDouble)}.foreach(println(_))
    combine.map((k)=>(k._1,k._2._1/k._2._2.toDouble)).foreach(println(_))
  }
}
