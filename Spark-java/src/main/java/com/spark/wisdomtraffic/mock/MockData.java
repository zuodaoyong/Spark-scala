package com.spark.wisdomtraffic.mock;


import com.spark.common.CommonUtils;
import com.spark.common.TimeUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.*;
import java.util.*;

/**
 *  模拟数据  数据格式如下：
 *
 *    日期	      卡口ID		     摄像头编号  	车牌号	       拍摄时间	              车速	             道路ID   	   区域ID
 *   date	 monitor_id	 camera_id	 car	action_time		speed	road_id		area_id
 *
 *   monitor_flow_action
 *   monitor_camera_info
 */
public class MockData {


    public static String MONITOR_FLOW_ACTION ="./monitor_flow_action";
    public static String MONITOR_CAMERA_INFO ="./monitor_camera_info";

    public static void main(String[] args){

        SparkConf sparkConf=new SparkConf().setAppName(MockData.class.getSimpleName()).setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);

        SparkSession sparkSession=SparkSession.builder().config(sparkConf).getOrCreate();
        List<Row> rowList = mockRowList(sc);
        Dataset<Row> flowActionDataset = mockFlowAction(sc, sparkSession, rowList);
        Dataset<Row> cameraInfo = mockCameraInfo(sc, sparkSession, rowList);
    }

    public static List<Row> mockRowList(JavaSparkContext sc){
        List<Row> dataList=new ArrayList<>();
        Random random=new Random();
        String[] locations = new String[]{"鲁","京","京","京","沪","京","京","深","京","京"};
        //date :如：2020-01-01
        String date=TimeUtils.parseToFormatTime(new Date(),TimeUtils.DAYSTR);
        /**
         * 模拟3000个车辆
         */
        for (int i = 0; i < 3000; i++) {
            //模拟车牌号：如：京A00001
            String car = locations[random.nextInt(10)] + (char)(65+random.nextInt(26))+ CommonUtils.fulfuill(5,random.nextInt(100000)+"");
            //baseActionTime 模拟24小时
            String baseActionTime = date + " " + CommonUtils.fulfuill(random.nextInt(24)+"");//2020-01-01 01
            /**
             * 模拟每辆车经过不同的卡扣不同的摄像头 数据
             */
            for(int j = 0 ; j < (random.nextInt(300)+1) ; j++){
                //模拟每个车辆每被30个摄像头拍摄后 时间上累计加1小时。这样做使数据更加真实。
                if(j % 30 == 0 && j != 0){
                    baseActionTime = date + " " + CommonUtils.fulfuill((Integer.parseInt(baseActionTime.split(" ")[1])+1)+"");
                }

                String actionTime = baseActionTime + ":"
                        + CommonUtils.fulfuill(random.nextInt(60)+"") + ":"
                        + CommonUtils.fulfuill(random.nextInt(60)+"");//模拟经过此卡扣开始时间 ，如：2020-01-01 20:09:10


                String monitorId = CommonUtils.fulfuill(4, random.nextInt(9)+"");//模拟9个卡扣monitorId，0补全4位

                String speed = (random.nextInt(260)+1)+"";//模拟速度

                String roadId = random.nextInt(50)+1+"";//模拟道路id 【1~50 个道路】

                String cameraId = CommonUtils.fulfuill(5, random.nextInt(100000)+"");//模拟摄像头id cameraId

                String areaId = CommonUtils.fulfuill(2,random.nextInt(8)+1+"");//模拟areaId 【一共8个区域】

                //将数据写入到文件中
                String content = date+"\t"+monitorId+"\t"+cameraId+"\t"+car+"\t"+actionTime+"\t"+speed+"\t"+roadId+"\t"+areaId;
                WriteDataToFile(MONITOR_FLOW_ACTION,content);
                Row row = RowFactory.create(date, monitorId, cameraId, car, actionTime, speed, roadId, areaId);
                dataList.add(row);
            }
        }
        return dataList;
    }

    public static Dataset<Row> mockFlowAction(JavaSparkContext sc,SparkSession sparkSession,List<Row> dataList){
        JavaRDD<Row> dataRowRDDs = sc.parallelize(dataList);

        StructType structType = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("monitor_id", DataTypes.StringType, true),
                DataTypes.createStructField("camera_id", DataTypes.StringType, true),
                DataTypes.createStructField("car", DataTypes.StringType, true),
                DataTypes.createStructField("action_time", DataTypes.StringType, true),
                DataTypes.createStructField("speed", DataTypes.StringType, true),
                DataTypes.createStructField("road_id", DataTypes.StringType, true),
                DataTypes.createStructField("area_id", DataTypes.StringType, true)
        ));
        Dataset<Row> dataFrame = sparkSession.createDataFrame(dataRowRDDs, structType);
        dataFrame.show();
        return dataFrame;

    }

    public static Dataset<Row> mockCameraInfo(JavaSparkContext sc,SparkSession sparkSession,List<Row> dataList){
        Random random=new Random();
        /**
         * monitorAndCameras    key：monitor_id
         * 						value:hashSet(camera_id)
         * 基于生成的数据，生成对应的卡扣号和摄像头对应基本表
         */
        Map<String,Set<String>> monitorAndCameras = new HashMap<>();
        int index = 0;
        for(Row row : dataList){
            //row.getString(1) monitor_id
            Set<String> sets = monitorAndCameras.get(row.getString(1));
            if(sets == null){
                sets = new HashSet<>();
                monitorAndCameras.put((String)row.getString(1), sets);
            }
            //这里每隔1000条数据随机插入一条数据，模拟出来标准表中卡扣对应摄像头的数据比模拟数据中多出来的摄像头。这个摄像头的数据不一定会在车辆数据中有。即可以看出卡扣号下有坏的摄像头。
            index++;
            if(index % 1000 == 0){
                sets.add(CommonUtils.fulfuill(5, random.nextInt(100000)+""));
            }
            //row.getString(2) camera_id
            sets.add(row.getString(2));
        }
        dataList.clear();
        Set<Map.Entry<String,Set<String>>> entrySet = monitorAndCameras.entrySet();
        for (Map.Entry<String, Set<String>> entry : entrySet) {
            String monitor_id = entry.getKey();
            Set<String> sets = entry.getValue();
            Row row = null;
            for (String camera_id : sets) {
                //将数据写入到文件
                String content = monitor_id+"\t"+camera_id;
                WriteDataToFile(MONITOR_CAMERA_INFO,content);
                row = RowFactory.create(monitor_id,camera_id);
                dataList.add(row);
            }
        }

        StructType monitorSchema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("monitor_id", DataTypes.StringType, true),
                DataTypes.createStructField("camera_id", DataTypes.StringType, true)
        ));

        JavaRDD<Row> dataRowRDDs = sc.parallelize(dataList);
        Dataset<Row> monitorDF = sparkSession.createDataFrame(dataRowRDDs, monitorSchema);
        return monitorDF;
    }

    public static void WriteDataToFile(String pathFileName,String newContent){
        FileOutputStream fos = null ;
        OutputStreamWriter osw = null;
        PrintWriter pw = null ;
        try {
            //产生一行模拟数据
            String content = newContent;
            File file = new File(pathFileName);
            fos=new FileOutputStream(file,true);
            osw=new OutputStreamWriter(fos, "UTF-8");
            pw =new PrintWriter(osw);
            pw.write(content+"\n");
            //注意关闭的先后顺序，先打开的后关闭，后打开的先关闭
            pw.close();
            osw.close();
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
