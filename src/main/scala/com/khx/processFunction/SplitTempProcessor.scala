package com.khx.processFunction

import com.khx.apitest.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * SplitTempProcessor
 *
 * @author kehaoxiong
 * @date 2020/11/3
 */
class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {

    // 判断当前数据的温度值，如果大于阈值，输出到主流；如果小于阈值，输出到侧输出流
    if (value.temperature > threshold) {
      out.collect(value)
    } else
      ctx.output(new OutputTag[(String, Double, Long)]("low-temp"), (value.id, value.temperature, value.timestamp))
  }
}
