package com.scala.spark.local.v1.rdd

import java.util.{Set,HashSet,Collections}

import org.apache.spark.util.AccumulatorV2

class LogAccumulator extends AccumulatorV2 [String,Set[String]]{

  private val logArray:Set[String]=new HashSet[String]()
  //累加器内部数据结构是否为空
  override def isZero: Boolean = {
    logArray.isEmpty
  }

  //产生一个新的累加器实例
  override def copy(): AccumulatorV2[String,Set[String]] = {
    val newAcc = new LogAccumulator()
    logArray.synchronized{
      newAcc.logArray.addAll(logArray)
    }
    newAcc
  }

  //重置累加器的数据结构
  override def reset(): Unit = {
    logArray.clear()
  }

  //修改累加器的方法
  override def add(v: String): Unit = {
    logArray.add(v)
  }

  //合并多个分区的累加器实例
  override def merge(other: AccumulatorV2[String,Set[String]]): Unit = {
    other match {
      case o:LogAccumulator=>logArray.addAll(o.value);
    }
  }

  //输出累加器的最终结果
  override def value: Set[String] = {
    Collections.unmodifiableSet(logArray)
  }
}
