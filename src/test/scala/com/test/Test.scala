package com.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


object Test {

  def main(args: Array[String]): Unit = {



    val spark=new SparkConf().setAppName("").setMaster("local[*]")
    val sc=new SparkContext(spark)

    //sc.textFile()

  }



}

