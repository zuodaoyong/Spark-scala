package com.test

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


object Test {

  def main(args: Array[String]): Unit = {
      val sparkConf= new SparkConf().setAppName(Test.getClass.getSimpleName).setMaster("local[*]")
      val sparkContext=new SparkContext(sparkConf)
      val rdd=sparkContext.makeRDD(List(1,2,3,4))
       rdd.cache()
       val result=rdd.map(_*2).reduce(_+_)

  }



}

