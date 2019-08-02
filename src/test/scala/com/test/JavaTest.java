package com.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class JavaTest {

    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf().setAppName(JavaTest.class.getSimpleName()).setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        JavaRDD<Integer> data = sc.parallelize(Arrays.asList(new Integer[]{1, 2, 3, 4}));
        data.repartition(100).mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Integer>>() {
            @Override
            public Iterator<Integer> call(Integer v1, Iterator<Integer> v2) throws Exception {
                List<Integer> list=new ArrayList<>();
                while (v2.hasNext()){
                    list.add(v2.next());
                }
                System.out.println(v1+":"+list);
                return list.iterator();
            }
        },false).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println();
            }
        });
    }
}
