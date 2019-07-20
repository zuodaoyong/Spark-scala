package com.spark.userbehavior.application;

import com.spark.userbehavior.mock.MockData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class UserVisitSessionAnalyze {
    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf().setAppName(UserVisitSessionAnalyze.class.getSimpleName())
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
        //生成测试数据
        MockData.mock(javaSparkContext,getSparkSession());
    }

    public static SparkSession getSparkSession(){
        return SparkSession.builder().getOrCreate();
    }
}
