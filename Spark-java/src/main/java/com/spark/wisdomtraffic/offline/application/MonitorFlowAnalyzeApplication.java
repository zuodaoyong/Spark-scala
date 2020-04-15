package com.spark.wisdomtraffic.offline.application;

import com.spark.config.HiveConfig;
import com.spark.wisdomtraffic.mock.MockDataToHive;
import com.spark.wisdomtraffic.offline.accumulators.MonitorAndCameraStateAccumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MonitorFlowAnalyzeApplication {

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkConf sparkConf=new SparkConf();
        sparkConf.setAppName(MockDataToHive.class.getSimpleName()).setMaster("local[*]");
        SparkConf hiveConfig = HiveConfig.setHiveConfig(sparkConf);
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        SparkSession sparkSession = SparkSession.builder().config(hiveConfig)
                .enableHiveSupport()
                .getOrCreate();

        //自定义累加器
        sc.accumulable("",new MonitorAndCameraStateAccumulator());

        JavaRDD<Row> flowActionrowJavaRDD = getFlowActionrowJavaRDD(sparkSession);
        JavaPairRDD<String, Row> flowActionMonitorIdWithRow = getFlowActionMonitorIdWithRow(flowActionrowJavaRDD);
        //flowActionMonitorIdWithRow持久化
        flowActionMonitorIdWithRow.cache();
        //按照卡扣号分组
        JavaPairRDD<String, Iterable<Row>> monitorIdIterableJavaPairRDD = flowActionMonitorIdWithRow.groupByKey();
        //monitorIdIterableJavaPairRDD持久化
        monitorIdIterableJavaPairRDD.cache();


    }


    private static JavaPairRDD<String,Row> getFlowActionMonitorIdWithRow(JavaRDD<Row> javaRDD){
        return javaRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            @Override
            public Iterator<Tuple2<String, Row>> call(Iterator<Row> rowIterator) throws Exception {
                List<Tuple2<String, Row>> list=new ArrayList<>();
                while (rowIterator.hasNext()){
                    Row next = rowIterator.next();
                    String monitor_id = next.getAs("monitor_id");
                    Tuple2<String, Row> tuple2=new Tuple2<String, Row>(monitor_id,next);
                    list.add(tuple2);
                }
                return list.iterator();
            }
        });
    }

    /**
     * 从hive里获取flowAction原始数据
     * @param sparkSession
     * @return
     */
    private static JavaRDD<Row> getFlowActionrowJavaRDD(SparkSession sparkSession){
        sparkSession.sql("use wisdomtraffic");
        Dataset<Row> rowDataset = sparkSession.sql("select * from monitor_flow_action");
        rowDataset.show();
        JavaRDD<Row> flowActionrowJavaRDD = rowDataset.toJavaRDD();
        return flowActionrowJavaRDD;
    }
}
