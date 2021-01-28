package com.khx.sink

import java.util.Properties

import com.khx.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
 * kafkaSinkTest
 *
 * @author kehaoxiong
 * @date 2020/10/27
 */
object kafkaSinkTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    val outStream = inputStream.map(string => {
      val dataArray = string.split(",")
      SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble).toString
    })

    inputStream.print()
    outStream.addSink( new FlinkKafkaProducer011[String]("localhost:9092","sinkTest",new SimpleStringSchema()))

    env.execute()
  }

}
