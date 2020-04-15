package com.spark.test.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;

/**
 * RDD的创建
 */
public class RDDCreate {

    public static void main(String[] args) {

        SparkConf sparkConf=new SparkConf().
                setAppName(RDDCreate.class.getSimpleName()).
                setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd;
        JavaPairRDD<String, String> pairRDD;
        //读取集合：
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList(new String[]{"java", "spark", "scala"}));
        rdd=rdd1;
        //读取文件，当然也可以是hdfs文件  hdfs://master/a/b
        JavaRDD<String> rdd2 = sc.textFile("D:\\temp\\file\\spark\\rdd\\json.txt",6);
        rdd=rdd2;
        rdd.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });




    }
}
