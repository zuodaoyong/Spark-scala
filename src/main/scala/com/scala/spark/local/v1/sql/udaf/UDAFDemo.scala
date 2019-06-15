package com.scala.spark.local.v1.sql.udaf

import com.scala.spark.local.v1.sql.RDDToDataFrameDemo
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object UDAFDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName(RDDToDataFrameDemo.getClass.getSimpleName)
    val sparkSession=SparkSession.builder().config(sparkConf).getOrCreate()
    //weakUDAF(sparkSession)
    udaf(sparkSession)
  }

  def udaf(sparkSession:SparkSession): Unit ={
    import sparkSession.implicits._
    val rdd=sparkSession.sparkContext.makeRDD(List(("neo",1000),("tom",1500),("jack",2100)))
    val userDS=rdd.map(tuple=>Employee(tuple._1,tuple._2)).toDS()
    val avgSalary=AvgSalary.toColumn.name("avgSalary")
    val result=userDS.select(avgSalary)
    result.show()
  }
  def weakUDAF(sparkSession:SparkSession): Unit ={
    import sparkSession.implicits._
    sparkSession.udf.register("weekAvgSalary",WeakAvgSalary)
    val rdd=sparkSession.sparkContext.makeRDD(List(("neo",1000),("tom",1500),("jack",2100)))
    val userDF=rdd.map(tuple=>Employee(tuple._1,tuple._2)).toDF()
    userDF.createOrReplaceTempView("employees")
    val result=sparkSession.sql("select weekAvgSalary(salary) from employees")
    result.show()
  }
}
