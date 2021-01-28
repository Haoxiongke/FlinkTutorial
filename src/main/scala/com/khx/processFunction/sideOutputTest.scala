package com.khx.processFunction

import com.khx.apitest.SensorReading
import org.apache.flink.streaming.api.scala._


/**
 * sideOutputTest
 *
 * @author kehaoxiong
 * @date 2020/11/3
 */
object sideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val host = "localhost"
    val port = 7777

    val inputStream = env.socketTextStream(host, port)
    val dataStream: DataStream[SensorReading] = inputStream
        .map(date => {
          val dataArray = date.split(",")
          SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
        })

    val highTempStream = dataStream
        .process(new SplitTempProcessor(30))

    val lowTempSteam = highTempStream.getSideOutput(new OutputTag[(String, Double, Long)]("low-temp"))

    highTempStream.print("high")
    lowTempSteam.print("low")

    env.execute("side output job")


  }

}
