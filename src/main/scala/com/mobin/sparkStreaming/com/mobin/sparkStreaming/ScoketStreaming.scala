package com.mobin.sparkStreaming.com.mobin.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hadoop on 3/28/16.
  */
object ScoketStreaming {

  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("local[2]").setAppName("ScoketStreaming")
    val sc = new StreamingContext(conf,Seconds(10))

    val lines = sc.socketTextStream("master",9998)
    val words = lines.flatMap(_.split((" ")))
    val wordCounts = words.map(x => (x , 1)).reduceByKey(_ + _)
    wordCounts.print()
    sc.start()
    sc.awaitTermination()
  }
}
