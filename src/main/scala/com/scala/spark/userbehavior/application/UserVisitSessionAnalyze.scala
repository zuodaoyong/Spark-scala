package com.scala.spark.userbehavior.application

import com.scala.spark.userbehavior.constants.Constants
import com.scala.spark.userbehavior.mock.MockData
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object UserVisitSessionAnalyze {

  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName("UserVisitSessionAnalyze")
      .setMaster("local[*]")
    val sparkContext=new SparkContext(sparkConf)
    val sparkSession=SparkSession.builder().getOrCreate()
    //生成测试数据
    MockData.mock(sparkContext,sparkSession)
    val rddRow:RDD[Row]=getActionRDDByDateRange(sparkSession, "2019-07-20", "2019-09-01")
    //rddRow.take(10).toStream.foreach(e=>println(e))
    val sessionid2FullAggrInfoRDD=aggregateBySession(sparkSession,rddRow)
    sessionid2FullAggrInfoRDD.take(10).toStream.foreach(e=>println(e))
  }

  /**
    * 获取指定日期范围内的用户访问行为数据
    * @param sparkSession
    * @param startDate
    * @param endDate
    * @return
    */
  def getActionRDDByDateRange(sparkSession: SparkSession,startDate:String,endDate:String): RDD[Row] ={
    val sql = "select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'"
    val dataSet:Dataset[Row]=sparkSession.sql(sql)
    dataSet.rdd
  }

  def aggregateBySession(sparkSession: SparkSession,actionRDD:RDD[Row]):RDD[Tuple2[String,String]]={
    val sessionIdRowRDD=actionRDD.map(row=>(row.getAs[String]("session_id").toString,row))
    //对行为数据按session粒度进行分组
    val sessionIdIterableTupleRDD=sessionIdRowRDD.groupByKey()
    val useridPartAggrInfoRDD:RDD[Tuple2[Long, String]]=sessionIdIterableTupleRDD.map(tuple=>{
       val sessionid=tuple._1
       val iterator=tuple._2.iterator
       val searchKeywordsBuffer=ListBuffer[String]()
       val clickCategoryIdsBuffer=ListBuffer[String]()
       var userid:Long=0L
      // 遍历session所有的访问行为
      while (iterator.hasNext){
        val row:Row=iterator.next()
        if(userid==0L){
          userid=row.getAs[Long]("user_id")
        }
        val searchKeyword=row.getAs[String]("search_keyword")
        val clickCategoryId=row.getAs[Long]("click_category_id")
        if(StringUtils.isNotEmpty(searchKeyword)){
          if(!searchKeywordsBuffer.contains(searchKeyword)){
            searchKeywordsBuffer.append(searchKeyword + ",")
          }
        }
        if(clickCategoryId!=null){
          if(!clickCategoryIdsBuffer.contains(clickCategoryId+"")){
            clickCategoryIdsBuffer.append(clickCategoryId + ",")
          }
        }

      }

      val searchKeywords = com.scala.spark.userbehavior.mock.StringUtils.trimComma(searchKeywordsBuffer.toString)
      val clickCategoryIds = com.scala.spark.userbehavior.mock.StringUtils.trimComma(clickCategoryIdsBuffer.toString)
      val partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|" + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds
      Tuple2[Long, String](userid, partAggrInfo)
    })

    // 查询所有用户数据，并映射成<userid,Row>的格式
    val sql="select * from user_info"
    val userInfoRDD:RDD[Row] =sparkSession.sql(sql).rdd
    val userid2InfoRDD:RDD[Tuple2[Long,Row]]=userInfoRDD.map(row=>(row.getAs[Long]("user_id"),row))
    // 将session粒度聚合数据，与用户信息进行join
    val useridFullInfoRDD:RDD[Tuple2[Long, Tuple2[String, Row]]]=useridPartAggrInfoRDD.join(userid2InfoRDD)
    val sessionid2FullAggrInfoRDD=useridFullInfoRDD.map(tuple=>{
      val partAggrInfo=tuple._2._1
      val userInfoRow=tuple._2._2
      val sessionid=com.scala.spark.userbehavior.mock.StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID)
      val age= userInfoRow.getAs[Int]("age")
      val professional=userInfoRow.getAs[String]("professional")
      val city=userInfoRow.getAs[String]("city")
      val sex = userInfoRow.getAs[String]("sex")
      val fullAggrInfo = partAggrInfo + "|"+ Constants.FIELD_AGE + "=" + age + "|"+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"+ Constants.FIELD_CITY + "=" + city + "|"+ Constants.FIELD_SEX + "=" + sex
      (sessionid, fullAggrInfo)
    })
    sessionid2FullAggrInfoRDD
  }

}
