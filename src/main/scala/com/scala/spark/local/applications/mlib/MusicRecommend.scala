package com.scala.spark.local.applications.mlib

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

case class MusicPlayInfo(userId:Int,artistId:Int,num:Int)
object MusicRecommend {

  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName(MusicRecommend.getClass.getSimpleName)
    val sparkSession=SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    val rdd=sparkSession.sparkContext.textFile("D:\\software\\temp\\spark\\user_artist_data.txt")
    var df=rdd.map(line=>line.split("\\s")).map(arr=>MusicPlayInfo(arr(0).toInt,arr(1).toInt,arr(2).toInt)).toDF()
    //计算userId和artistId的最大值和最小值
    df.agg(("userId","min"),("userId","max"),("artistId","min"),("artistId","max")).show()
  }
}
