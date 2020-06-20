package com.scala.spark.project.movie

import com.scala.spark.project.movie.MovieUserAnalyzerApplication.getUserRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object MovieUserAnalyzerDataFrameApplication {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]")
      .setAppName(MovieUserAnalyzerDataFrameApplication.getClass.getSimpleName())
    //sparkConf.set("spark.default.parallelism","1")

    //val sc = new SparkContext(sparkConf)

    implicit val sparkSession=SparkSession.builder().config(sparkConf).getOrCreate()
    val sc=sparkSession.sparkContext
    val userDF=getUserDF(sparkSession)
    val ratingDF=getRatingDF(sparkSession)
    val movieDF=getMovieDF(sparkSession)

    //movie_gender_age_info(userDF,ratingDF)

    movieTopRatingAndTopView(ratingDF)





  }

  //所有电影中平均评分最高的，观看人数最多的电影
  def movieTopRatingAndTopView(ratingDF:DataFrame)(implicit sparkSession: SparkSession): Unit ={

    val df=ratingDF.select("movieId","rating").groupBy("movieId").avg("rating").withColumnRenamed("avg(rating)","rating")
    df.sort(df("rating").desc).limit(20).sort(df("movieId")).show()
    //ratingDF.createOrReplaceTempView("rating")

    //sparkSession.sql("select movieId,avg from (select movieId,avg(rating) as avg from rating group by movieId order by avg desc limit 20) t order by movieId").show()
  }

  /**
   * 特定电影男性,女性年龄分布
   * @param userDF
   * @param ratingDF
   */
  def movie_gender_age_info(userDF:DataFrame,ratingDF:DataFrame): Unit ={
    var df=ratingDF.filter("movieId=2858")
      .join(userDF,"userId")
      .select("userId","gender","age")
      .groupBy("gender","age").count()
    df.sort(df("count").desc).show()
  }


  def getMovieDF(sparkSession: SparkSession):DataFrame={
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
    //定义scheme
    val movieScheme=StructType(
      Seq(
          StructField("id",IntegerType,false),
        StructField("title",StringType,true),
        StructField("movieType",StringType,true)
      )
    )
    val movieRow=getMovieRDD(sparkSession.sparkContext).map(movie=>Row(movie.id,movie.title,movie.movieType))
    sparkSession.createDataFrame(movieRow,movieScheme)
  }



  def getUserDF(sparkSession:SparkSession):DataFrame={
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
    //定义schema
    val userSchema=StructType(
      Seq(
        StructField("userId",IntegerType,false),
        StructField("gender",StringType,true),
        StructField("age",IntegerType,true),
        StructField("occupationId",StringType,true)
      )
    )
    val userRow=getUserRDD(sparkSession.sparkContext).map(user=>Row(user.userId,user.gender,user.age,user.occupationId))
    sparkSession.createDataFrame(userRow,userSchema)
  }


  def getRatingDF(sparkSession: SparkSession):DataFrame={
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
    //定义schema  userId:Int,movieId:Int,rating:Double,timeStamp:Long
    val ratingScheme=StructType(
      Seq(
         StructField("userId",IntegerType,false),
        StructField("movieId",IntegerType,true),
        StructField("rating",DoubleType,true),
        StructField("timeStamp",LongType,true)
      )
    )
    val ratingRow=getRatingRDD(sparkSession.sparkContext).map(rating=>Row(rating.userId,rating.movieId,rating.rating,rating.timeStamp))
    sparkSession.createDataFrame(ratingRow,ratingScheme)
  }


}
