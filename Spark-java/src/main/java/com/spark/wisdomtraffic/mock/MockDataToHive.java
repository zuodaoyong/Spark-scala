package com.spark.wisdomtraffic.mock;

import com.spark.config.HiveConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class MockDataToHive {

    public static void main(String[] args){
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkConf sparkConf=new SparkConf();
        sparkConf.setAppName(MockDataToHive.class.getSimpleName()).setMaster("local[*]");
        SparkConf hiveConfig = HiveConfig.setHiveConfig(sparkConf);
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        SparkSession sparkSession = SparkSession.builder().config(hiveConfig)
                .enableHiveSupport()
                .getOrCreate();
        System.out.println(sparkSession);

        sparkSession.sql("use wisdomtraffic");






        Dataset<Row> sql = sparkSession.sql("select * " +
                "from " +
                "(" +
                "select camera.monitor_id,camera.camera_id,flow.monitor_id as flow_mid,flow.camera_id as flow_cid " +
                "from monitor_camera_info camera " +
                "left join " +
                "monitor_flow_action flow " +
                "on camera.monitor_id=flow.monitor_id " +
                "and camera.camera_id=flow.camera_id " +
                ") t " +
                "where flow_mid is null or flow_cid is null");
        sql.show();
    }

}
