package com.spark.test.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.util.parsing.json.JSON;
import java.util.Arrays;

public class Test {


    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf();
        sparkConf.setAppName(Test.class.getSimpleName());
        sparkConf.setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);

        SparkSession sparkSession=SparkSession.builder().getOrCreate();

        Dataset<Row> load = sparkSession.read().
                format("jdbc")
                .option("url", "jdbc:mysql://47.99.106.249:3306/xcshop?useUnicode=true&characterEncoding=utf-8")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", "xc_goods_record")
                .option("user", "mysqltest_user")
                .option("password", "JswkFxrOy52#4yX1GNAd7")
                .load();

        load.show();

    }
}
