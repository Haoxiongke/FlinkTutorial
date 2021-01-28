package com.khx.state

import com.khx.apitest.SensorReading
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.KeyedStateBackend

/**
 * TempChangeWarning
 *
 * @author kehaoxiong
 * @date 2020/11/4
 */
class TempChangeWarning(threshold: Double) extends RichMapFunction[SensorReading, (String, Double, Double)] {

  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("temp-warning", classOf[Double]))
  }

  override def map(in: SensorReading): (String, Double, Double) = {
    // 从状态中取出上次的状态值
    val lastTemp = lastTempState.value()
    lastTempState.update(in.temperature)

    // 跟当前温度值计算差值，然后跟阈值进行比较，如果大于则报警
    val diff = (in.temperature - lastTemp).abs
    if (diff > threshold) {
      (in.id, lastTemp, in.temperature)
    } else
      null
  }
}
