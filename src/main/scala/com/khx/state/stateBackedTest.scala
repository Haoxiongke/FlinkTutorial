package com.khx.state

import com.khx.apitest.SensorReading
import org.apache.flink.streaming.api.scala._


/**
 * stateBackedTest
 *
 * @author kehaoxiong
 * @date 2020/11/4
 */
object stateBackedTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.socketTextStream("localhost", 7777)
    val resultStream = inputStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })
        .keyBy("id")
        //        .flatMap(new TempChangeWarningWithFlatMap(10.0)) // 通过实现接口，调用flatMap中的状态管理
        .flatMapWithState[(String, Double, Double), Double] {   // 调用flatMap定义好的state方法
          case (inputDate: SensorReading, None) => (List.empty, Some(inputDate.temperature))
          case (inputDate: SensorReading, lastTemp: Some[Double]) => {
            if ((inputDate.temperature - lastTemp.get).abs > 10.0) {
              (List((inputDate.id, lastTemp.get, inputDate.temperature)), Some(inputDate.temperature))
            } else {
              (List.empty, Some(inputDate.temperature))
            }
          }
        }
    resultStream.print("stateBacked")
    env.execute("StateBacked")
  }
}
