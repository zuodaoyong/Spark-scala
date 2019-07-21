package com.spark.userbehavior.application;

import com.spark.userbehavior.constants.Constants;
import com.spark.userbehavior.mock.MockData;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Iterator;

public class UserVisitSessionAnalyze {
    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf().setAppName(UserVisitSessionAnalyze.class.getSimpleName())
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
        SparkSession sparkSession = getSparkSession();
        //生成测试数据
        MockData.mock(javaSparkContext,getSparkSession());
        JavaRDD<Row> actionRDDByDateRange = getActionRDDByDateRange(sparkSession, "2019-07-20", "2019-09-01");
        JavaPairRDD<String, String> stringStringJavaPairRDD = aggregateBySession(sparkSession, actionRDDByDateRange);
        stringStringJavaPairRDD.take(10).stream().forEach(e-> System.out.println(e));

    }

    /**
     * 获取指定日期范围内的用户访问行为数据
     * @param sparkSession
     * @param startDate
     * @param endDate
     * @return
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SparkSession sparkSession,String startDate,String endDate){
         String sql="select * from user_visit_action where date>='"+startDate+"' and date<='"+endDate+"'";
         Dataset<Row> dataset = sparkSession.sql(sql);
         return dataset.toJavaRDD();
    }

    /**
     * 对行为数据按session粒度进行聚合
     * @param sparkSession
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, String> aggregateBySession(SparkSession sparkSession,JavaRDD<Row> actionRDD){
        JavaPairRDD<String, Row> sessionIdRowRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                String session_id = row.getAs("session_id").toString();
                return new Tuple2<String, Row>(session_id, row);
            }
        });
        //对行为数据按session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionIdIterableJavaPairRDD = sessionIdRowRDD.groupByKey();

        // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        // 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long, String> useridPartAggrInfoRDD=sessionIdIterableJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String sessionid = tuple._1;
                Iterator<Row> iterator = tuple._2.iterator();
                StringBuffer searchKeywordsBuffer = new StringBuffer("");
                StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
                Long userid = null;
                // 遍历session所有的访问行为
                while(iterator.hasNext()) {
                    // 提取每个访问行为的搜索词字段和点击品类字段
                    Row row = iterator.next();
                    if(userid == null) {
                        userid = Long.valueOf(row.getAs("user_id").toString());
                    }
                    String searchKeyword = row.getAs("search_keyword")!=null?row.getAs("search_keyword").toString():null;
                    Long clickCategoryId = row.getAs("click_category_id")!=null?Long.valueOf(row.getAs("click_category_id").toString()):null;

                    // 实际上这里要对数据说明一下
                    // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
                    // 其实，只有搜索行为，是有searchKeyword字段的
                    // 只有点击品类的行为，是有clickCategoryId字段的
                    // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

                    // 我们决定是否将搜索词或点击品类id拼接到字符串中去
                    // 首先要满足：不能是null值
                    // 其次，之前的字符串中还没有搜索词或者点击品类id
                    if(StringUtils.isNotEmpty(searchKeyword)) {
                        if(!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                            searchKeywordsBuffer.append(searchKeyword + ",");
                        }
                    }
                    if(clickCategoryId != null) {
                        if(!clickCategoryIdsBuffer.toString().contains(
                                String.valueOf(clickCategoryId))) {
                            clickCategoryIdsBuffer.append(clickCategoryId + ",");
                        }
                    }
                }
                String searchKeywords = com.spark.userbehavior.mock.StringUtils.trimComma(searchKeywordsBuffer.toString());
                String clickCategoryIds = com.spark.userbehavior.mock.StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                        + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                        + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds;

                return new Tuple2<Long, String>(userid, partAggrInfo);
            }
        });
        // 查询所有用户数据，并映射成<userid,Row>的格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sparkSession.sql(sql).toJavaRDD();
        JavaPairRDD<Long, Row> userid2InfoRDD=userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<Long, Row>(Long.valueOf(row.getAs("user_id").toString()), row);
            }
        });
        // 将session粒度聚合数据，与用户信息进行join
        JavaPairRDD<Long, Tuple2<String, Row>> useridFullInfoRDD = (JavaPairRDD<Long, Tuple2<String, Row>>) useridPartAggrInfoRDD.join(userid2InfoRDD);

        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD=useridFullInfoRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                String partAggrInfo = tuple._2._1;
                Row userInfoRow = tuple._2._2;

                String sessionid = com.spark.userbehavior.mock.StringUtils.getFieldFromConcatString(
                        partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
                int age = Integer.valueOf(userInfoRow.getAs("age").toString());
                String professional = userInfoRow.getAs("professional").toString();
                String city = userInfoRow.getAs("city").toString();
                String sex = userInfoRow.getAs("sex").toString();
                String fullAggrInfo = partAggrInfo + "|"
                        + Constants.FIELD_AGE + "=" + age + "|"
                        + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                        + Constants.FIELD_CITY + "=" + city + "|"
                        + Constants.FIELD_SEX + "=" + sex;
                return new Tuple2<String, String>(sessionid, fullAggrInfo);
            }
        });
       return sessionid2FullAggrInfoRDD;
    }

    public static SparkSession getSparkSession(){
        return SparkSession.builder().getOrCreate();
    }
}
