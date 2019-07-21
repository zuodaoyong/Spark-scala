package com.scala.spark.userbehavior.accumulators

import com.scala.spark.userbehavior.constants.Constants
import org.apache.commons.lang3.StringUtils
import org.apache.spark.util.AccumulatorV2
class SessionAggrStatAccumulator extends AccumulatorV2[String,String]{

  var result= Constants.SESSION_COUNT + "=0|" + Constants.TIME_PERIOD_1s_3s + "=0|" + Constants.TIME_PERIOD_4s_6s + "=0|" + Constants.TIME_PERIOD_7s_9s + "=0|" + Constants.TIME_PERIOD_10s_30s + "=0|" + Constants.TIME_PERIOD_30s_60s + "=0|" + Constants.TIME_PERIOD_1m_3m + "=0|" + Constants.TIME_PERIOD_3m_10m + "=0|" + Constants.TIME_PERIOD_10m_30m + "=0|" + Constants.TIME_PERIOD_30m + "=0|" + Constants.STEP_PERIOD_1_3 + "=0|" + Constants.STEP_PERIOD_4_6 + "=0|" + Constants.STEP_PERIOD_7_9 + "=0|" + Constants.STEP_PERIOD_10_30 + "=0|" + Constants.STEP_PERIOD_30_60 + "=0|" + Constants.STEP_PERIOD_60 + "=0"

  /**
    * 当AccumulatorV2中存在类似数据不存在这种问题时，是否结束程序
    * @return
    */
  override def isZero: Boolean = true

  /**
    * 拷贝一个新的AccumulatorV2
    * @return
    */
  override def copy(): AccumulatorV2[String, String] = {
    val sessionAggrStatAccumulator=new SessionAggrStatAccumulator()
    sessionAggrStatAccumulator.result=this.result
    sessionAggrStatAccumulator
  }

  /**
    * 重置AccumulatorV2中的数据
    */
  override def reset(): Unit = {
    result= Constants.SESSION_COUNT + "=0|" + Constants.TIME_PERIOD_1s_3s + "=0|" + Constants.TIME_PERIOD_4s_6s + "=0|" + Constants.TIME_PERIOD_7s_9s + "=0|" + Constants.TIME_PERIOD_10s_30s + "=0|" + Constants.TIME_PERIOD_30s_60s + "=0|" + Constants.TIME_PERIOD_1m_3m + "=0|" + Constants.TIME_PERIOD_3m_10m + "=0|" + Constants.TIME_PERIOD_10m_30m + "=0|" + Constants.TIME_PERIOD_30m + "=0|" + Constants.STEP_PERIOD_1_3 + "=0|" + Constants.STEP_PERIOD_4_6 + "=0|" + Constants.STEP_PERIOD_7_9 + "=0|" + Constants.STEP_PERIOD_10_30 + "=0|" + Constants.STEP_PERIOD_30_60 + "=0|" + Constants.STEP_PERIOD_60 + "=0"
  }

  /**
    *  操作数据累加方法实现
    * @param v
    */
  override def add(v: String): Unit = {
    val v1=this.result
    val v2=v
    // 使用StringUtils工具类，从v1中，提取v2对应的值，并累加1
    if(StringUtils.isEmpty(v1)){
      this.result=v2
    }
    val oldValue = com.scala.spark.userbehavior.mock.StringUtils.getFieldFromConcatString(v1, "\\|", v2)
    if (oldValue != null) { // 将范围区间原有的值，累加1
      val newValue = oldValue.toInt + 1
      // 使用StringUtils工具类，将v1中，v2对应的值，设置成新的累加后的值
      this.result= com.scala.spark.userbehavior.mock.StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue))
    }
  }

  /**
    * 合并数据
    * @param other
    */
  override def merge(other: AccumulatorV2[String, String]): Unit = {
     other match{
      case map:SessionAggrStatAccumulator=>result=other.value
      case _=>throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
  }

  /**
    *  AccumulatorV2对外访问的数据结果
    * @return
    */
  override def value: String = result
}
