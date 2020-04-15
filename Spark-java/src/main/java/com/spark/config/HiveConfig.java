package com.spark.config;

import org.apache.spark.SparkConf;

public class HiveConfig {

    public static SparkConf setHiveConfig(SparkConf sparkConf){
        sparkConf.set("spark.sql.warehouse.dir","hdfs://master:9000/user/hive/warehouse")
                 .set("hive.metastore.warehouse.dir","hdfs://master:9000/user/hive/warehouse")
                 .set("spark.sql.catalogImplementation","hive")
                 .set("hive.metastore.uris","thrift://master:9083");
        return sparkConf;
    }
}
