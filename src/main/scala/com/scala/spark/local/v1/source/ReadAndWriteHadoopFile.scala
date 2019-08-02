package com.scala.spark.local.v1.source

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{JobConf, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用HadoopFile读写hdfs
  */
object ReadAndWriteHadoopFile {

  def main(args: Array[String]): Unit = {
    val sparkConf= new SparkConf().setAppName(ReadAndWriteHadoopFile.getClass.getSimpleName).setMaster("local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryoserializer.classesToRegister","org.apache.hadoop.io.Text,org.apache.hadoop.io.LongWritable")
    val sparkContext=new SparkContext(sparkConf)
    //testHadoopFile(sparkContext)
    //newAPIHadoopFile(sparkContext)
    val jobConf=new JobConf();
  }


  /**
    * 使用hadoopFile读取文件，InputFormat必须是老版本hadoop接口
    * @param sparkContext
    */
  def testHadoopFile(sparkContext:SparkContext): Unit ={
    val rdd=sparkContext.hadoopFile[LongWritable, Text,TextInputFormat]("hdfs://10.100.191.156:8020/test/flow.txt")
    rdd.foreach(print(_))
  }

  /**
    * 使用newAPIHadoopFile读取文件,InputFormat必须是新版本hadoop接口
    * @param sparkContext
    */
  def newAPIHadoopFile(sparkContext: SparkContext): Unit ={
    val rdd=sparkContext.newAPIHadoopFile[Text,Text,KeyValueTextInputFormat]("hdfs://10.100.191.156:8020/test/flow.txt")
    val rdd2=sparkContext.newAPIHadoopFile[LongWritable,Text,org.apache.hadoop.mapreduce.lib.input.TextInputFormat]("hdfs://10.100.191.156:8020/test/flow.txt")
    rdd.foreach{(t)=>{
      println(t._1+"-------"+t._2)
    }}
    rdd.foreach{case(k,v)=>{
      println(k+"-------"+v)
    }}
    rdd2.foreach((tuple)=>{
      print(tuple._1+"------"+tuple._2)
    })
  }
}
