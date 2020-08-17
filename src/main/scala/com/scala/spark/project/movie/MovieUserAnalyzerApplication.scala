package com.scala.spark.project.movie

import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
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
case class SecondarySortKey(val first:Double,val second:Double) extends Ordered[SecondarySortKey] with Serializable{
  override def compare(that: SecondarySortKey): Int = {
    if(this.first!=that.first){
      (this.first-that.first).toInt
    }else{
      if(this.second-that.second>0){
          Math.ceil(this.second-that.second).toInt
      }else if(this.second-that.second<0){
          Math.floor(this.second-that.second).toInt
      }else{
        (this.second-that.second).toInt
      }
    }
  }
}
object MovieUserAnalyzerApplication {

  def main(array: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[1]")
      .setAppName(MovieUserAnalyzerApplication.getClass.getSimpleName())
    sparkConf.set("spark.default.parallelism","1")
    val sc = new SparkContext(sparkConf)

    sc.setLogLevel("ERROR")
    val userRDD=getUserRDD(sc)
    val movieRDD=getMovieRDD(sc)
    val ratingRDD=getRatingRDD(sc)

    //getUserByMovie(userRDD,ratingRDD,movieRDD)
    //getTop10Movie(ratingRDD)
    //getViewMovie(ratingRDD)
    //maleLoveMovie(userRDD,ratingRDD)
    //getFemaleLoveMovie(userRDD,ratingRDD)
    //getUserGroup(sc,userRDD,ratingRDD,movieRDD)
    movieRatingSecondKeySort(ratingRDD)
  }


  //对电影评分进行二次排序
  def movieRatingSecondKeySort(ratingRDD:RDD[Rating]): Unit ={

    val sortKey=ratingRDD.map(rating=>(new SecondarySortKey(rating.timeStamp.toDouble,rating.rating),rating))
    sortKey.sortByKey(false).foreach(println(_))


  }

  //用户分群
  def getUserGroup(sc:SparkContext,userRDD:RDD[User],ratingRDD:RDD[Rating],movieRDD:RDD[Movie]): Unit ={
    val qq=userRDD.filter(x=>x.age==18).map(x=>(x.userId,x.age))
    val wechat=userRDD.filter(x=>x.age==25).map(x=>(x.userId,x.age))
    val qqSet=mutable.HashSet() ++ qq.map(_._1).collect()
    val wechatSet=mutable.HashSet()++ wechat.map(_._1).collect()
    val qqBroadCast=sc.broadcast(qqSet)
    val wechatBroadCast=sc.broadcast(wechatSet)

    val qq_Rating=ratingRDD.filter(x=>qqBroadCast.value.contains(x.userId)).map(x=>(x.movieId,1))
    val movie=movieRDD.map(x=>(x.id,x.title))
    val qq_movie:RDD[(Int,(String,Int))]=movie.join(qq_Rating)
    qq_movie.map(x=>x._2).reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey(false).foreach(println(_))
  }

  //最受女性喜欢的电影
  def getFemaleLoveMovie(userRDD:RDD[User],ratingRDD:RDD[Rating]): Unit ={
    val user=userRDD.filter(x=>x.gender=="F").map(x=>(x.userId,1))
    val rating=ratingRDD.map(x=>(x.userId,x.movieId))
    user.join(rating).map(x=>(x._2._2,x._2._1)).reduceByKey(_+_).map(x=>(x._2,x._1))
      .sortByKey(false).foreach(println(_))
  }

  //最受男性喜欢的电影
  def maleLoveMovie(userRDD:RDD[User],ratingRDD:RDD[Rating]): Unit ={
    val user=userRDD.filter(x=>x.gender=="M")
      .map(x=>(x.userId,1))
    val rating=ratingRDD.map(x=>(x.userId,x.movieId))
    val resRDD=user.join(rating)
    resRDD.map(x=>(x._2._2,x._2._1)).reduceByKey(_+_).map(x=>(x._2,x._1))
      .sortByKey(false).foreach(println(_))
  }

  //观看人数最多的人数
  def getViewMovie(ratingRDD:RDD[Rating]): Unit ={
    val rdd=ratingRDD.map(x=>(x.movieId,1))
    rdd.reduceByKey(_+_).map(x=>(x._2,x._1))
      .sortByKey(false).foreach(println(_))

  }

  //所有电影平均分最高的top10
  def getTop10Movie(ratingRDD: RDD[Rating]): Unit ={
    val movieRatingTuple:RDD[(Int,(Double,Int))]=ratingRDD.map(rating=>(rating.movieId,(rating.rating,1)))
    movieRatingTuple.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(x=>(x._2._1/x._2._2,x._1)).sortByKey(false)
      .take(100).foreach(println(_))
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
