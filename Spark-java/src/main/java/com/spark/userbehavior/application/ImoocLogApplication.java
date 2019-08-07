package com.spark.userbehavior.application;

import com.spark.userbehavior.mock.DateUtils;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
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
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.StringType;

public class ImoocLogApplication {
    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf().setAppName(ImoocLogApplication.class.getSimpleName()).setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
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
                    if(StringUtils.isNotEmpty(url)&&url.length()>domainBroadCast.value().length()){
                        String cms = url.substring(url.indexOf(domainBroadCast.value()) + domainBroadCast.value().length());
                        String[] cmsTypeId = cms.split("/");
                        if(cmsTypeId.length > 1) {
                            cmsType = cmsTypeId[0];
                            cmsId = Long.valueOf(cmsTypeId[1]);
                        }
                    }
                    String city="";
                    String day = time.substring(0,10).replaceAll("-","");
                    Row row = RowFactory.create(time, url, traffic,
                            ip, cmsType, cmsId,
                            city, day);
                    rows.add(row);
                }
                return rows.iterator();
            }
        });
        SparkSession sparkSession=SparkSession.builder().config(sparkConf).getOrCreate();

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
        dataFrame.show();
    }
}
