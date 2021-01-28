package com.khx.apitest

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

class MySensorSource() extends SourceFunction[SensorReading] {
  //
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {

    val random = new Random()

    val tempsCur = 1.to(10).map {
      data => {
        ("sensors_" + data, 60 + random.nextGaussian() * 20)
      }
    }

    while (running) {

      val timestamp = System.currentTimeMillis()

      val currTemp = tempsCur.map {
        data => (data._1, data._2 + random.nextGaussian())
      }

      currTemp.foreach(
        data => sourceContext.collect(SensorReading(data._1, timestamp, data._2))
      )

      Thread.sleep(1000)
    }

  }

  // 定义一个flag,表示数据源是否正常运行
  var running = true

  override def cancel(): Unit = running = false
}
