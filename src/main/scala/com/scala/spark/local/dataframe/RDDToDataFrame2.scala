package com.scala.spark.local.dataframe

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 编程方式把RDD转DataFrame
  */
object RDDToDataFrame2 {

  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName(RDDToDataFrame2.getClass.getSimpleName)
    val sparkContext=new SparkContext(sparkConf)
    val sqlContext=new SQLContext(sparkContext)
    val stuRDD=sparkContext.textFile("D:\\software\\temp\\spark\\stu.txt",1)
    //获取row的RDD
    val rowRDD=stuRDD.map(line=>line.split(","))
      .map(arr=>Row(arr(0).toInt,arr(1),arr(2).toInt))
    //构造元数据
    val structType=StructType(Array(StructField("id",IntegerType,true),StructField("name",StringType,true),StructField("age",IntegerType,true)))

    //rdd转DataFrame
    val stuDF=sqlContext.createDataFrame(rowRDD,structType)
    stuDF.registerTempTable("stu")
    val resultDF=sqlContext.sql("select * from stu where age>20")
    resultDF.foreach(row=>println(row.getAs[String]("name")))

  }
}
