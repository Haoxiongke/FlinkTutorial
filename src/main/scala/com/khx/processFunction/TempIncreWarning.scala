package com.khx.processFunction

import com.khx.apitest.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * TempIncreWarning
 *
 * @author kehaoxiong
 * @date 2020/10/30
 */
class TempIncreWarning(interval: Long) extends KeyedProcessFunction[Tuple, SensorReading, String] {

  // 需要上一个状态的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  // 需要上一个定时器的ts
  lazy val curTimerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("cur-time-ts", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 首先取出状态
    val lastTemp = lastTempState.value()
    val curTimerTs = curTimerTsState.value()

    lastTempState.update(value.temperature)

    // 如果当前温度值，比之前高，且没有定时器的话，注册10秒后的定时器
    if (value.temperature > lastTemp && curTimerTs == 0) {
      val ts = ctx.timerService().currentProcessingTime() + interval
      ctx.timerService().registerProcessingTimeTimer(ts)
      curTimerTsState.update(ts)
    } else if (value.temperature < lastTemp) {
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      curTimerTsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("温度值连续" + interval / 1000L + "秒上升")
    // 清空状态
    curTimerTsState.clear()
  }

}
