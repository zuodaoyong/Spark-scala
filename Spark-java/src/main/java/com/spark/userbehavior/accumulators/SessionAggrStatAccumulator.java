package com.spark.userbehavior.accumulators;

import com.spark.userbehavior.constants.Constants;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.util.AccumulatorV2;

public class SessionAggrStatAccumulator extends AccumulatorV2<String,String> {
    String result= Constants.SESSION_COUNT + "=0|"
            + Constants.TIME_PERIOD_1s_3s + "=0|"
            + Constants.TIME_PERIOD_4s_6s + "=0|"
            + Constants.TIME_PERIOD_7s_9s + "=0|"
            + Constants.TIME_PERIOD_10s_30s + "=0|"
            + Constants.TIME_PERIOD_30s_60s + "=0|"
            + Constants.TIME_PERIOD_1m_3m + "=0|"
            + Constants.TIME_PERIOD_3m_10m + "=0|"
            + Constants.TIME_PERIOD_10m_30m + "=0|"
            + Constants.TIME_PERIOD_30m + "=0|"
            + Constants.STEP_PERIOD_1_3 + "=0|"
            + Constants.STEP_PERIOD_4_6 + "=0|"
            + Constants.STEP_PERIOD_7_9 + "=0|"
            + Constants.STEP_PERIOD_10_30 + "=0|"
            + Constants.STEP_PERIOD_30_60 + "=0|"
            + Constants.STEP_PERIOD_60 + "=0";
    @Override
    public boolean isZero() {
        return true;
    }

    @Override
    public AccumulatorV2<String, String> copy() {
        SessionAggrStatAccumulator sessionAggrStatAccumulator=new SessionAggrStatAccumulator();
        sessionAggrStatAccumulator.result=this.result;
        return sessionAggrStatAccumulator;
    }

    @Override
    public void reset() {
        result= Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    @Override
    public void add(String v) {
        String v1= this.result;
        String v2=v;
        // 校验：v1为空的话，直接返回v2
        if (StringUtils.isEmpty(v1)){
            this.result=v2;
        }
        // 使用StringUtils工具类，从v1中，提取v2对应的值，并累加1
        String oldValue = com.spark.userbehavior.mock.StringUtils.getFieldFromConcatString(v1, "\\|", v2);
        if (oldValue != null) { // 将范围区间原有的值，累加1
            Integer newValue = Integer.valueOf(oldValue) + 1;
            // 使用StringUtils工具类，将v1中，v2对应的值，设置成新的累加后的值
            this.result= com.spark.userbehavior.mock.StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
        }
    }

    @Override
    public void merge(AccumulatorV2<String, String> other) {
        if(other instanceof SessionAggrStatAccumulator){
            this.result=((SessionAggrStatAccumulator) other).result;
        }else{
            throw new UnsupportedOperationException(
                    "Cannot merge ${this.getClass.getName} with ${other.getClass.getName}");
        }
    }

    @Override
    public String value() {
        return this.result;
    }
}
