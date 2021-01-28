package com.khx.project

import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests
import org.slf4j.LoggerFactory

/**
 * LogAnalysis
 *
 * @author kehaoxiong
 * @date 2021/1/25
 */
object LogAnalysis {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 必须设置检查点，要不然无法写入es
    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //        env.setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //    properties.setProperty("auto.offset.reset", "latest")

    val logger = LoggerFactory.getLogger("LogAnalysis")

    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), properties))

    val logData = inputStream.map(x => {
      val fields = x.split("\t")

      val level = fields(2)
      val timeStr = fields(3)
      var time = 0L

      try {
        val sourceFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        time = sourceFormat.parse(timeStr).getTime
      } catch {
        case e: Exception => logger.error(s"time parse error: $timeStr", e.getMessage)
        //        case e: Exception => print("time parse error: $timeStr")
      }

      val domain = fields(5)
      val traffic = fields(6).toLong

      (level, time, domain, traffic)
    })
        .filter(_._2 != 0)
        .filter(_._1 == "E")
        .map(x => {
          (x._2, x._3, x._4)
        })

    val resultData = logData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, Long)] {

      val bound = 10000L
      var currentMaxTimestamp: Long = _

      override def getCurrentWatermark: Watermark = new Watermark(currentMaxTimestamp - bound)

      override def extractTimestamp(element: (Long, String, Long), previousElementTimestamp: Long): Long = {
        val timestamp = element._1
        currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp)
        timestamp
      }
    })
        .keyBy(1)
        .window(TumblingEventTimeWindows.of(Time.seconds(30)))
        .apply(new WindowFunction[(Long, String, Long), Traffic, Tuple, TimeWindow] {
          override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, Long)], out: Collector[Traffic]): Unit = {
            val domain = key.getField(0).toString
            var sum = 0L

            val iterator = input.iterator
            var inputTime = 0L
            while (iterator.hasNext) {
              val next = iterator.next()
              sum += next._3
              inputTime = next._1
            }

            val resultFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
            val time = resultFormat.format(inputTime)

            out.collect(Traffic(time, domain, sum))
          }
        })

    //定义es的连接信息
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("127.0.0.1", 9200))

    //自定义写入es的ElasticsearchSinkFunction
    val myEsSinkFunc = new ElasticsearchSinkFunction[Traffic] {
      override def process(t: Traffic, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        //定义一个map作为 数据源
        val dataSource = new util.HashMap[String, Any]()
        dataSource.put("time", t.time)
        dataSource.put("domain", t.domain)
        dataSource.put("traffics", t.traffics)

        //创建index request ，指定index
        val indexRequest = Requests.indexRequest()
        indexRequest.index("traffic") //指定写入哪一个索引
            .source(dataSource) //指定写入的数据
        //            .type("_doc")  //我这里用的es7已经不需要这个参数了

        //执行新增操作
        requestIndexer.add(indexRequest)
      }
    }

    resultData.addSink(new ElasticsearchSink.Builder[Traffic](httpHosts, myEsSinkFunc)
        .build()
    )

    env.execute()
  }
}
