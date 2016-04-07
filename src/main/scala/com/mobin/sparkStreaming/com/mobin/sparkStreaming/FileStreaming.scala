package com.mobin.sparkStreaming.com.mobin.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hadoop on 3/29/16.
  */
object FileStreaming {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("FileStreaming")
    val sc = new StreamingContext(conf,Seconds(5))
    val lines = sc.textFileStream("/home/hadoop/word")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x , 1)).reduceByKey(_ + _)
    sc.start()
    sc.awaitTermination()
  }
}
