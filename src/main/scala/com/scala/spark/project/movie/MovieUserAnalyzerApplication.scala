package com.scala.spark.project.movie

import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

case class User(userId: Int, //用户id
                gender: String, //性别
                age: Int, //年龄
                occupationId: String //职业
               )

case class Movie(id: Int, //电影id
                 title: String, //电影名称
                 movieType: String //电影类型
                )
case class Rating(
                 userId:Int,movieId:Int,rating:Double,timeStamp:Long
                 )
object MovieUserAnalyzerApplication {

  def main(array: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]")
      .setAppName(MovieUserAnalyzerApplication.getClass.getSimpleName())
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    val userRDD=getUserRDD(sc)
    val movieRDD=getMovieRDD(sc)
    val ratingRDD=getRatingRDD(sc)
    getUserByMovie(userRDD,ratingRDD,movieRDD)

  }
  //某部电影观看的用户信息
  def getUserByMovie(userRDD:RDD[User],ratingRDD:RDD[Rating],movieRDD:RDD[Movie]){
    val userIdUser=userRDD.map(user=>(user.userId,user))
    val movieIdMovie=movieRDD.map(movie=>(movie.id,movie))
    val userIdRating=ratingRDD.map(rating=>(rating.userId,rating))
    val userIdRatingUser:RDD[(Int,User)]=userIdRating.join(userIdUser).map(tuple=>(tuple._2._1.movieId,tuple._2._2)).groupBy(tuple=>tuple).map(tuple=>tuple._1)

    val movieIdRating=ratingRDD.map(rating1=>(rating1.movieId,rating1))
    val movieIdRatingMovie:RDD[(Int,Movie)]=movieIdRating.join(movieIdMovie).map(tuple=>(tuple._1,tuple._2._2)).groupBy(tuple=>tuple).map(tuple=>tuple._1)
    val res:RDD[(Int,(User,Movie))]=userIdRatingUser.join(movieIdRatingMovie)
  }

  def getMovieRDD(sc:SparkContext):RDD[Movie]={
    val movieRdd = sc.textFile("D:\\temp\\spark\\file\\movie\\movies.dat")
    movieRdd.mapPartitions(iter=>{
      val listBuffer=new ListBuffer[Movie]
      while (iter.hasNext){
        val line=iter.next()
        val lines=line.split("::")
        listBuffer+=Movie(lines(0).toInt,lines(1),lines(2))
      }
      listBuffer.iterator
    })

  }

  def getUserRDD(sc:SparkContext): RDD[User] = {
    val userRdd = sc.textFile("D:\\temp\\spark\\file\\movie\\users.dat")
    userRdd.mapPartitions(it => {
      val userList = new ListBuffer[User];
      while (it.hasNext) {
        val line = it.next()
        val lines = line.split("::")
        userList += User(lines(0).toInt, lines(1), lines(2).toInt, lines(3))
      }
      userList.iterator
    })
  }

  def getRatingRDD(sc:SparkContext):RDD[Rating]={
    val ratingRdd=sc.textFile("D:\\temp\\spark\\file\\movie\\ratings.dat")
    ratingRdd.mapPartitions(iter=>{
      val listBuffer=new ListBuffer[Rating]
      while (iter.hasNext){
        val line=iter.next()
        val lines=line.split("::")
        listBuffer+=Rating(lines(0).toInt,lines(1).toInt,lines(2).toDouble,lines(3).toLong)
      }
      listBuffer.iterator
    })
  }

}
