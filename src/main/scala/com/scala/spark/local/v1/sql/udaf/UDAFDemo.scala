package com.scala.spark.local.v1.sql.udaf

import com.scala.spark.local.v1.sql.RDDToDataFrameDemo
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
case class Employee(name:String,salary: Long)
object UDAFDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName(RDDToDataFrameDemo.getClass.getSimpleName)
    val sparkSession=SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    sparkSession.udf.register("avgSalary",AvgSalary)
    val rdd=sparkSession.sparkContext.makeRDD(List(("neo",1000),("tom",1500),("jack",2100)))
    val userDF=rdd.map(tuple=>Employee(tuple._1,tuple._2)).toDF()
    userDF.createOrReplaceTempView("employees")
    val result=sparkSession.sql("select avgSalary(salary) from employees")
    result.show()
  }
}
