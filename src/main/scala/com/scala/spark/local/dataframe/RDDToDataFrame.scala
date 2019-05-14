package com.scala.spark.local.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

case class stu(id:Int,name:String,age:Int)
object RDDToDataFrame {

  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("a")
    val sparkContext=new SparkContext(sparkConf);
    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._
    val stuRDD=sparkContext.textFile("E:\\temp\\spark\\stu.txt",1)
    val df=stuRDD.map(line=>line.split(","))
      .map(arr=>stu(arr(0).toInt,arr(1),arr(2).toInt))
      .toDF()

    df.createTempView("stu")
    val resultDF=sqlContext.sql("select * from stu where age>20")
    resultDF.show()
    val resultRDD=resultDF.rdd
    resultRDD.map(row=>stu(row.getAs[Int]("id"),row.getAs[String]("name"),row.getAs[Int]("age")))
      .foreach(e=>print(e.id+":"+e.name+":"+e.age))
  }

}
