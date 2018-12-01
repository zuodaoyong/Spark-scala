package com.scala.spark.product.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object DataFrameCreate {
  
  def main(args:Array[String]):Unit={
    
    val conf=new SparkConf().setAppName("DataFrameCreate").setMaster("local");
    val context=new SparkContext(conf);
    val sqlContext=new SQLContext(context);
    sqlContext.read.json("src\\main\\resources\\sql\\students.json").show();
  }
}