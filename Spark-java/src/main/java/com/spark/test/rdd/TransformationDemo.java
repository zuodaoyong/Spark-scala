package com.spark.test.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.Test;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * RDD的转换
 */
public class TransformationDemo implements Serializable {



    public JavaSparkContext initSC(){
        SparkConf sparkConf=new SparkConf().setMaster("local[*]").setAppName(TransformationDemo.class.getSimpleName());
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        return sc;
    }

    public JavaRDD<Integer> initRDD(){
        JavaSparkContext sc=initSC();
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(new Integer[]{1, 2, 3, 4}));
        return rdd;
    }

    public JavaPairRDD<String, Integer> initPairRDD(){
        JavaSparkContext sc=initSC();
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(new Integer[]{1, 2, 3, 4}));
        JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(new PairFunction<Integer, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Integer integer) throws Exception {
                String key="a";
                if(integer%2==0){
                   key="b";
                }
                return new Tuple2<String, Integer>(key, integer);
            }
        });
        return pairRDD;
    }

    @Test
    public void mapPartitions(){
        JavaRDD<Integer> rdd = initRDD();
        JavaRDD<Integer> mapPartitions = rdd.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Integer> iterator) throws Exception {
                List<Integer> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    list.add(iterator.next() * 2);
                }
                return list.iterator();
            }
        });

        mapPartitions.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    @Test
    public void partitionsWithIndex(){
        JavaRDD<Integer> rdd = initRDD();
        JavaRDD<Integer> partitionsWithIndex = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Integer>>() {
            @Override
            public Iterator<Integer> call(Integer integer, Iterator<Integer> iterator) throws Exception {
                System.out.println();
                System.out.print("分区号：" + integer+",分区数据：");
                while (iterator.hasNext()) {
                    System.out.print(iterator.next() + " ");
                }
                System.out.println();
                return iterator;
            }
        }, true);
        partitionsWithIndex.foreachPartition(new VoidFunction<Iterator<Integer>>() {
            @Override
            public void call(Iterator<Integer> iterator) throws Exception {
                //System.out.println(iterator);
            }
        });
    }


    @Test
    public void sample(){
        JavaRDD<Integer> rdd = initRDD();
        JavaRDD<Integer> sample = rdd.sample(false, 0.2, 1);
        sample.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.print(integer+" ");
            }
        });
    }

    @Test
    public void aggregateByKey(){
        JavaPairRDD<String, Integer> pairRDD = initPairRDD();
        JavaPairRDD<String, Integer> aggregateByKey = pairRDD.aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        aggregateByKey.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1+"@"+stringIntegerTuple2._2);
            }
        });
    }


    @Test
    public void cogroupTest(){
        JavaSparkContext sc = initSC();
        JavaRDD<Tuple2> rdd1 = sc.parallelize(Arrays.asList(new Tuple2(1, "zs"), new Tuple2(2, "ls"), new Tuple2(3, "ww")));
        JavaRDD<Tuple2> rdd2=sc.parallelize(Arrays.asList(new Tuple2(1,90),new Tuple2(2,85),new Tuple2(3,100)));
        //rdd1.cogroup(rdd2).foreach(println(_))
    }
}
