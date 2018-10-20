package com.scala.spark.local

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Transformation {
  val sentences=Array[String](
		"hello java hello word",
		"hello spark hello scala"
  );
  val students=Array(
			("class1",90),
			("class2",70),
			("class3",80),
			("class1",86)
	);
  val scores=Array(
			Tuple2(90,"tom"),
			Tuple2(70,"jack"),
			Tuple2(80,"abc"),
			Tuple2(98,"neo")
	);
  
  val snos=Array(
			Tuple2(1,"tom"),
			Tuple2(2,"jack"),
			Tuple2(3,"abc"),
			Tuple2(4,"neo")
	);
	val id_scores=Array(
			Tuple2(1,90),
			Tuple2(2,70),
			Tuple2(3,80),
			Tuple2(4,98)
	);
	
  def main(args:Array[String]):Unit={
    val conf=new SparkConf().setAppName("Transformation").setMaster("local");
    val context=new SparkContext(conf);
    //map(context)
    //filter(context)
    //flatMap(context)
    //groupByKey(context)
    //reduceByKey(context)
    //sortByKey(context)
    //join(context)
    cogroup(context)
  }
  
  /**
   * cogroup算子
   */
  def cogroup(context:SparkContext){
    val scordRdd=context.parallelize(id_scores,1)
    val snoRdd=context.parallelize(snos,1)
    scordRdd.cogroup(snoRdd).foreach(e=>println(e._1+":"+e._2))
  }
  
  /**
   * join算子
   */
  def join(context:SparkContext){
    val scordRdd=context.parallelize(id_scores,1)
    val snoRdd=context.parallelize(snos,1)
    scordRdd.join(snoRdd)
    .foreach(e=>println(e._1+":"+e._2))
  }
  
  /**
   * sortByKey算子
   */
  def sortByKey(context:SparkContext){
    context.parallelize(scores,1)
           .sortByKey(false,1)
           .foreach(e=>println(e._1+":"+e._2))
  }
  
  /**
   * reduceByKey算子
   */
  def reduceByKey(context:SparkContext){
    context.parallelize(students,1)
           .reduceByKey(_+_)
           .foreach(e=>println(e._1+":"+e._2))
  }
  
  def groupByKey(context:SparkContext){
    context.parallelize(students,1)
           .groupByKey()
           .foreach(e=>println(e._1+":"+e._2))
  }
  
  def flatMap(context:SparkContext){
    context.parallelize(sentences, 1)
           .flatMap(line=>line.split(" "))
           .foreach(word=>println(word))
  }
  
  /**
   * filter算子
   */
  def filter(context:SparkContext){
    context.parallelize(1 to 9,1)
           .filter(item=>item%2==0)
           .foreach(t=>print(t+" "))
  }
  
  /**
   * map算子
   */
  def map(context:SparkContext){
    context.parallelize(1 to 9,2)
           .map(e=>e<<1)
           .foreach(t=>print(t+" "))
  }
}