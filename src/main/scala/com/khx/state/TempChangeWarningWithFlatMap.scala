package com.khx.state

import com.khx.apitest.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.util.Collector

/**
 * TempChangeWarningWithFlatMap
 *
 * @author kehaoxiong
 * @date 2020/11/4
 */
class TempChangeWarningWithFlatMap(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  lazy val lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))

  override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    // 从状态中取出上次的状态值
    val lastTemp = lastTempState.value()
    lastTempState.update(in.temperature)

    // 跟当前温度值计算差值，然后跟阈值进行比较，如果大于则报警
    val diff = (in.temperature - lastTemp).abs
    if (diff > threshold) {
      collector.collect((in.id, lastTemp, in.temperature))
    }
  }

}
