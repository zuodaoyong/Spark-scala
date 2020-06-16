package com.scala.spark

import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql.SparkSession

object Test {

  def main(args: Array[String]): Unit = {
    val sparkSession=SparkSession.builder().master("local[*]").appName(Test.getClass.getSimpleName)
      .getOrCreate()
    var sc=sparkSession.sparkContext
    sc.setLogLevel("warn")
    import sparkSession.implicits._
    val lines=sparkSession.readStream.format("socket")
      .option("host","localhost")
      .option("port",9999)
      .load()

    val words=lines.as[String].flatMap(line=>line.split(" "))
    val wordcount=words.groupBy("value").count()

    val query=wordcount.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()
  }

}
