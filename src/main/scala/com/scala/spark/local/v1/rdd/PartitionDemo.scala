package com.scala.spark.local.v1.rdd

import com.scala.spark.local.v1.partitions.CustomPartitioner
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PartitionDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName(PartitionDemo.getClass.getSimpleName)
    val sparkContext=new SparkContext(sparkConf)
    val rdd = sparkContext.makeRDD(List((1,"a"),(2,"b"),(3,"c"),(4,"d")),8)
    //rdd.collect().foreach(println(_))
    //HashPartitioner在数据量大时，有可能会出现数据倾斜
    //val rdd2=rdd.partitionBy(new HashPartitioner(3))
    var rdd3=rdd.partitionBy(new CustomPartitioner(2))
    rdd3.collect().foreach(println(_))
  }
}
