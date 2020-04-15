package com.spark.wisdomtraffic.offline.accumulators;

import com.spark.common.CommonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.AccumulatorParam;

public class MonitorAndCameraStateAccumulator implements AccumulatorParam<String> {

    /**
     * t1就是上次累加后的结果,第一次调用的时候就是zero方法return的值,t2是传进来的字符串
     * @param t1
     * @param t2
     * @return
     */
    @Override
    public String addAccumulator(String t1, String t2) {
        return myAdd(t1, t2);
    }

    /**
     * addAccumulator方法之后，最后会执行这个方法，将每个分区最后的value加到初始化的值。
     * 这里的r1就是我们初始化的值那个“”。r2是已经经过addAccumulator这个方法累加后每个分区处理的值。
     */
    @Override
    public String addInPlace(String r1, String r2) {
        return myAdd(r1,r2);
    }

    /**
     *初始化RDD每个分区的值
     * @param initialValue 调用SparkContext.accululator时传递的initialValue，就是""
     * @return 返回累加器每个分区中的初始值。
     */
    @Override
    public String zero(String initialValue) {
        System.out.println("init *************"+initialValue);
        return "normalMonitorCount"+"=0|"
                + "normalCameraCount"+"=0|"
                + "abnormalMonitorCount"+"=0|"
                + "abnormalCameraCount"+"=0|"
                + "abnormalMonitorCameraInfos"+"= ";
    }

    private String myAdd(String v1, String v2) {
        if(StringUtils.isEmpty(v1)){
            return v2;
        }
        //abnormalMonitorCount=1|abnormalCameraCount=100|abnormalMonitorCameraInfos="0002":07553,07554,07556
        String[] valArr = v2.split("\\|");
        for (String string : valArr) {
            String[] fieldAndValArr = string.split("=");
            String field = fieldAndValArr[0];
            String value = fieldAndValArr[1];
            String oldVal = CommonUtils.getFieldFromConcatString(v1, "\\|", field);
            if(oldVal != null){
                //只有这个字段是string，所以单独拿出来
                if("abnormalMonitorCameraInfos".equals(field)){
                    v1 = CommonUtils.setFieldInConcatString(v1, "\\|", field, oldVal + "~" + value);
                }else{
                    //其余都是int类型，直接加减就可以
                    int newVal = Integer.parseInt(oldVal)+Integer.parseInt(value);
                    v1 = CommonUtils.setFieldInConcatString(v1, "\\|", field, String.valueOf(newVal));
                }
            }
        }
        return v1;
    }

}
