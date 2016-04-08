package com.mobin.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hadoop on 4/2/16.
  */
object WindowWordCount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("windowWordCunt")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("master",9998)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x , 1)).reduceByKeyAndWindow(_+_,_+_,Seconds(60),Seconds(10))
    wordCounts.print
    ssc.start()
    ssc.awaitTermination()
  }


}
