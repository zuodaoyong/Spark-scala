package com.scala.spark.local.v1.partitions

import org.apache.spark.Partitioner

class CustomPartitioner(numPars:Int) extends Partitioner{
  override def numPartitions: Int = numPars

  override def getPartition(key: Any): Int = {
     key.toString.toInt%this.numPartitions
  }
}
