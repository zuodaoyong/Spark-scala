package com.scala.spark.local.v1.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
case class Stu(name:String,age:Int)
object RDDToDataFrameDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName(RDDToDataFrameDemo.getClass.getSimpleName)
    val sparkSession=SparkSession.builder().config(sparkConf).getOrCreate()
    //fun1(sparkSession)
    //fun2(sparkSession)
    fun3(sparkSession)
  }

  /**
    * rdd转dataFrame方式3
    * @param sparkSession
    */
  def fun3(sparkSession:SparkSession): Unit ={
    var stuRDD=sparkSession.sparkContext.makeRDD(List(("neo",20),("tom",25)))
    var rowRDD=stuRDD.map(stu=>Row(stu._1,stu._2))
    val schema=StructType(
               List(
                 StructField("name1",StringType,true),
                 StructField("age",IntegerType,true)
               )
    )
    val stuDF=sparkSession.createDataFrame(rowRDD,schema)
    stuDF.show()
  }

  /**
    * rdd转dataFrame方式2
    * @param sparkSession
    */
  def fun2(sparkSession:SparkSession): Unit ={
    var stuRDD=sparkSession.sparkContext.makeRDD(List(("neo",20),("tom",25)))
    import sparkSession.implicits._
    var stuDF=stuRDD.map(tuple=>Stu(tuple._1,tuple._2.toInt)).toDF()
    stuDF.show()
  }

  /**
    * rdd转dataFrame方式1
    * @param sparkSession
    */
  def fun1(sparkSession:SparkSession): Unit ={
    var stuRDD=sparkSession.sparkContext.makeRDD(List(("neo",20),("tom",25)))
    import sparkSession.implicits._
    val stuDF=stuRDD.toDF("name","age")
    stuDF.show()
  }
}
