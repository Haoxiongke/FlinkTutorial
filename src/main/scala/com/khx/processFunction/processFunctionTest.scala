package com.khx.processFunction

import com.khx.apitest.SensorReading
import org.apache.flink.streaming.api.scala._

/**
 * processFunctionTest
 *
 * @author kehaoxiong
 * @date 2020/10/30
 */
object processFunctionTest {

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

    val warningStream = dataStream
        .keyBy("id")
        .process(new TempIncreWarning(10000L))

    warningStream.print()
    env.execute("process function job")

  }

}
