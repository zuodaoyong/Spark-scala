package com.scala.spark.local.v1.sql.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * 弱类型聚合
  */
object WeakAvgSalary extends UserDefinedAggregateFunction{

  //输入参数的数据类型
  override def inputSchema: StructType = StructType(StructField("salary",LongType,true)::Nil)
  //保存业务数据的数据结构
  override def bufferSchema: StructType = StructType(StructField("sum",LongType,true)::StructField("count",IntegerType,true)::Nil)
  //返回值的数据类型
  override def dataType: DataType = DoubleType
  //相同的输入有相同的输出
  override def deterministic: Boolean = true
  //初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 存工资的总额
    buffer(0) = 0L
    // 存工资的个数
    buffer(1) = 0
  }
  //同分区内row对聚合函数的操作
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      buffer(0)=buffer.getLong(0)+input.getLong(0)
      buffer(1)=buffer.getInt(1)+1
    }
  }
  //不同分区的聚合结果的合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getLong(0)+buffer2.getLong(0)
    buffer1(1)=buffer1.getInt(1)+buffer2.getInt(1)
  }
  //计算最终结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble/buffer.getInt(1)
  }
}
