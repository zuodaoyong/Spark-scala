package com.spark.test.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class BroadCastDemo {
    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf();
        sparkConf.setMaster("local[*]").setAppName(BroadCastDemo.class.getSimpleName());
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        List<Integer> list= Arrays.asList(1,2,3);
        Broadcast<List<Integer>> broadcast = sc.broadcast(list);
        sc.parallelize(Arrays.asList("hello java","hello scala","hello pythton"))
                .mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
                    @Override
                    public Iterator<String> call(Iterator<String> iterator) throws Exception {
                        List<String> result=new ArrayList<>();
                        while (iterator.hasNext()){
                            System.out.println(broadcast.getValue());
                            String line = iterator.next();
                            String[] s = line.split(" ");
                            for (int i = 0; i <s.length ; i++) {
                                result.add(s[i]);
                            }
                        }
                        return result.iterator();
                    }
                }).foreachPartition(e->{

        });
    }
}
