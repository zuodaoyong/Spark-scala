package com.spark.wisdomtraffic.offline.application;

import com.spark.config.HiveConfig;
import com.spark.wisdomtraffic.mock.MockDataToHive;
import com.spark.wisdomtraffic.offline.accumulators.MonitorAndCameraStateAccumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
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


    /**
     * 遍历分组后的RDD，拼接字符串
     * 数据中一共就有9个monitorId信息，那么聚合之后的信息也是9条
     * monitor_id=|cameraIds=|area_id=|camera_count=|carCount=
     * 例如:
     * ("0005","monitorId=0005|areaId=02|camearIds=09200,03243,02435,03232|cameraCount=4|carCount=100")
     *
     */
    private static JavaPairRDD<String,String> aggreagteByMonitor(JavaPairRDD<String, Iterable<Row>> monitorIdIterableJavaPairRDD){

        return monitorIdIterableJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
                String monitorId=stringIterableTuple2._1;
                Iterable<Row> rowIterable = stringIterableTuple2._2;
                Iterator<Row> rowIterator = rowIterable.iterator();
                //同一个monitorId下，对应的所有的不同的cameraId
                List<String> list=new ArrayList<>();
                //同一个monitorId下，对应的所有的不同的camearId信息
                StringBuilder tmpInfos = new StringBuilder();
                String areaId = "";
                //统计车辆数的count
                int count = 0;
                while (rowIterator.hasNext()){
                    Row next = rowIterator.next();
                    areaId = next.getString(7);
                    String cameraId = next.getString(2);
                    if(!list.contains(cameraId)){
                        list.add(cameraId);
                    }
                    if(!tmpInfos.toString().contains(cameraId)){
                        tmpInfos.append(","+cameraId);
                    }
                    //这里的count就代表的车辆数，一个row一辆车
                    count++;
                }
                int cameraCount = list.size();
                //monitorId=0001|areaId=03|cameraIds=00001,00002,00003|cameraCount=3|carCount=100
                String infos =  "monitorId="+monitorId+"|"
                        +"areaId="+areaId+"|"
                        +"cameraIds="+tmpInfos.toString().substring(1)+"|"
                        +"cameraCount="+cameraCount+"|"
                        +"carCount="+count;
                return new Tuple2<String, String>(monitorId, infos);
            }
        });

    }
    /**
     * flowAction转换成JavaPairRDD<MonitorId，row>
     * @param javaRDD
     * @return
     */
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
