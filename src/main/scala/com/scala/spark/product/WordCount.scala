package com.scala.spark.product

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().
      setAppName(WordCount.getClass.getSimpleName)
      .set("spark.cores.max","2")
    val sparkContext=new SparkContext(sparkConf)
    val rdd=sparkContext.parallelize(List("hello word","hello java","hello spark"))
    val rdd2=rdd.flatMap(line=>
          {Thread.sleep(1000*60)
            line.split(" ")})
      .map(word=>(word,1))
        .reduceByKey(_+_)
    rdd2.foreach(e=>println(e._1+":"+e._2))
  }
}
