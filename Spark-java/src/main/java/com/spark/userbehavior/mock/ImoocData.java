package com.spark.userbehavior.mock;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ImoocData {

    public static void mock(JavaSparkContext sc, SparkSession sparkSession){
        String domain="http://www.imooc.com/";
        Broadcast<String> domainBroadCast=sc.broadcast(domain);
        JavaRDD<String> sourceRDD = sc.textFile("src\\main\\resources\\access.log");
        JavaRDD<Row> sourceRow = sourceRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Row>() {
            @Override
            public Iterator<Row> call(Iterator<String> iterator) throws Exception {
                List<Row> rows=new ArrayList<>();
                while (iterator.hasNext()){
                    String next = iterator.next();
                    String[] splits = next.split("\\s");
                    String ip = splits[0];
                    String time = DateUtils.parse(splits.length>4?(splits[3]+" "+splits[4]):"");
                    String url=splits.length>11?splits[11].replaceAll("\"",""):"";
                    Long traffic=0L;
                    if(splits.length>9){
                        traffic=Long.valueOf(splits[9]);
                    }
                    String cmsType = "";
                    Long cmsId = 0l;
                    if(org.apache.commons.lang3.StringUtils.isNotEmpty(url)&&url.length()>domainBroadCast.value().length()){
                        String cms = url.substring(url.indexOf(domainBroadCast.value()) + domainBroadCast.value().length());
                        String[] cmsTypeId = cms.split("/");
                        if(cmsTypeId.length > 1) {
                            cmsType = cmsTypeId[0];
                            if(StringUtils.isNumeric(cmsTypeId[1])){
                                cmsId = Long.valueOf(cmsTypeId[1]);
                            }
                        }
                    }
                    String city="";
                    String day="";
                    if(time!=null&&time.length()>10){
                        day = time.substring(0,10).replaceAll("-","");
                    }
                    Row row = RowFactory.create(time, url, traffic,
                            ip, cmsType, cmsId,
                            city, day);
                    rows.add(row);
                }
                return rows.iterator();
            }
        });
        StructType scheme = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("time", DataTypes.StringType, true),
                DataTypes.createStructField("url", DataTypes.StringType, true),
                DataTypes.createStructField("traffic", DataTypes.LongType, true),
                DataTypes.createStructField("ip", DataTypes.StringType, true),
                DataTypes.createStructField("cmsType", DataTypes.StringType, true),
                DataTypes.createStructField("cmsId", DataTypes.LongType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("day", DataTypes.StringType, true)
                )
        );
        Dataset<Row> dataFrame = sparkSession.createDataFrame(sourceRow, scheme);
        //dataFrame.coalesce(1).write().format("parquet").partitionBy("day").save("D:\\software\\temp\\spark\\offline\\");
        dataFrame.coalesce(1).write().format("parquet").partitionBy("day").save("hdfs://home0:9000/spark/imooc");
    }
}
