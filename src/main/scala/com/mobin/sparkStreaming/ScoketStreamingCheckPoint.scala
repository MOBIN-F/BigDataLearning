package com.mobin.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Mobin on 2017/8/25.
  */
object ScoketStreamingCheckPoint {
  val conf = new SparkConf().setMaster("local[*]").setAppName("checkPoint")
  val checkPointPath = "."

  def checkPointFun(): StreamingContext = {
       val sc = new StreamingContext(conf, Seconds(5))
       val lines = sc.socketTextStream("localhost",9998)
       sc.checkpoint(checkPointPath)
       val words = lines.flatMap(_.split((" ")))
       val wordCounts = words.map(x => (x , 1)).reduceByKey(_ + _)
       wordCounts.print()
       sc
  }

  def main(args: Array[String]) {
    val context = StreamingContext.getOrCreate(checkPointPath, checkPointFun)
    context.start()
    context.awaitTermination()
  }
}
