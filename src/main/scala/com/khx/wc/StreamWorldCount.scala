package com.khx.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWorldCount {
  def main(args: Array[String]): Unit = {
    //创建流处理的执行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val params = ParameterTool.fromArgs(args)
    val host = params.get("host")
    val port = params.getInt("port")

    //接受socket文本流
    val inputStream = environment.socketTextStream(host, port)

    val resultDataStream = inputStream
        .flatMap(_.split(" "))
        .filter(_.nonEmpty)
        .map((_, 1))
        .keyBy(0)
        .sum(1).setParallelism(1)

    resultDataStream.print()
    environment.execute("stream word count job")
  }
}
