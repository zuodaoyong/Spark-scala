package com.scala.spark.local.v1.rdd

import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName(CheckPointerDemo.getClass.getSimpleName)
    val sparkContext=new SparkContext(sparkConf)
    val accum = new LogAccumulator
    sparkContext.register(accum, "logAccum")
    val sum = sparkContext.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "8", "9"), 2).filter(line => {
      val pattern = """^-?(\d+)"""
      val flag = line.matches(pattern)
      if (!flag) {
        accum.add(line)
      }
      flag
    }).map(_.toInt).reduce(_ + _)

    println("sum: " + sum)
    //因为 Java 集合类型在 Scala 操作时没有 foreach 方法, 所以需要将其转换为Scala的集合类型,
    //因此需要在代码中加入如下内容(Scala支持与Java的隐式转换),
    import scala.collection.JavaConversions._
    for (v:String <- accum.value) print(v + " ")
    println()
    sparkContext.stop()
  }
}
