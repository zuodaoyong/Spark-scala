package com.scala.spark.local.v1.sql.udaf

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator



case class Employee(name:String,salary: Long)
case class Average(var sum:Long,var count:Int)
object AvgSalary extends Aggregator[Employee,Average,Double]{
  //定义一个数据结构，保存工资总数和工资总个数，初始都为0
  override def zero: Average = Average(0L,0)

  override def reduce(b: Average, a: Employee): Average = {
     b.sum+=a.salary
     b.count+=1
     b
  }

  override def merge(b1: Average, b2: Average): Average = {
    b1.sum+=b2.sum
    b1.count+=b2.count
    b1
  }

  override def finish(reduction: Average): Double = reduction.sum.toDouble/reduction.count
  //Encoders.product是进行scala元组和case类转换的编码器
  override def bufferEncoder: Encoder[Average] = Encoders.product
  //最终输出值的编码器
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
