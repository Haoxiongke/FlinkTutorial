package com.khx.wc

import org.apache.flink.api.scala._

//import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment

    val inputFile: DataSet[String] = environment.readTextFile("/Users/kehaoxiong/IdeaProjects/FlinkTutorial/src/main/resources/word.txt")

    val resultDataSet = inputFile
        .flatMap(_.split(" "))
        .map((_, 1))
        .groupBy(0)
        .sum(1)

    resultDataSet.print()
  }
}
