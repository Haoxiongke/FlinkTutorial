package com.khx.project

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/**
 * ElasticSearchSinkTest
 *
 * @author kehaoxiong
 * @date 2021/1/27
 */
object ElasticSearchSinkTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream = env.readTextFile("src/main/resources/sensor.txt")
    env.setParallelism(1)
    inputStream.print()

    //先转换成样例类类型
    val dataStream = inputStream
        .map(data => {
          val arr = data.split(",") //按照,分割数据，获取结果
          SensorReadingTest5(arr(0), arr(1).toLong, arr(2).toDouble) //生成一个传感器类的数据，参数中传toLong和toDouble是因为默认分割后是字符串类别
        })

    //定义es的连接信息
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("127.0.0.1", 9200))

    //自定义写入es的ElasticsearchSinkFunction
    val myEsSinkFunc = new ElasticsearchSinkFunction[SensorReadingTest5] {
      override def process(t: SensorReadingTest5, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        //定义一个map作为 数据源
        val dataSource = new util.HashMap[String, String]()
        dataSource.put("id", t.id)
        dataSource.put("temperature", t.temperature.toString)
        dataSource.put("ts", t.timestamp.toString)

        //创建index request ，指定index
        val indexRequest = Requests.indexRequest()
        indexRequest.index("sensors") //指定写入哪一个索引
            .source(dataSource) //指定写入的数据
        //            .type("_doc")  //我这里用的es7已经不需要这个参数了

        //执行新增操作
        requestIndexer.add(indexRequest)
      }
    }

    dataStream.addSink(new ElasticsearchSink.Builder[SensorReadingTest5](httpHosts, myEsSinkFunc)
        .build()
    )
    env.execute()
  }
}

case class SensorReadingTest5(id: String, timestamp: Long, temperature: Double)