package com.spark.userbehavior.application;

import com.spark.userbehavior.mock.ImoocData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;


public class ImoocLogApplication {
    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf().setAppName(ImoocLogApplication.class.getSimpleName())
                .setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        SparkSession sparkSession=SparkSession.builder()
                .config(sparkConf)
                .config("spark.sql.sources.partitionColumnTypeInference.enabled","false")
                .getOrCreate();
        //生产数据
        ImoocData.mock(sc,sparkSession);
        Dataset<Row> parquetDS = sparkSession.read().parquet("hdfs://home0:9000/spark/imooc");
        //parquetDS.printSchema();
        parquetDS.show();
        parquetDS.cache();
        videoTopN(parquetDS);
        trafficTopN(parquetDS);
    }

    public static void videoTopN(Dataset<Row> parquetDS){
        Dataset<Row> aggDS = parquetDS.filter(new FilterFunction<Row>() {
            @Override
            public boolean call(Row value) throws Exception {
                String cmsType = value.getAs("cmsType").toString();
                if (cmsType.equals("video")) {
                    return true;
                }
                return false;
            }
        }).groupBy("day", "cmsId").agg(functions.count("cmsId").alias("num"));
        Dataset<Row> resultDS = aggDS.orderBy(aggDS.col("num").desc());
        resultDS.show();
    }

    public static void trafficTopN(Dataset<Row> parquetDS){
        Dataset<Row> aggDS = parquetDS.filter(new FilterFunction<Row>() {
            @Override
            public boolean call(Row value) throws Exception {
                String cmsType = value.getAs("cmsType").toString();
                if (cmsType.equals("video")) {
                    return true;
                }
                return false;
            }
        }).groupBy("day","cmsId").agg(functions.sum("traffic").alias("num"));
        Dataset<Row> resultDS = aggDS.orderBy(aggDS.col("num").desc());
        resultDS.show();
    }
}
