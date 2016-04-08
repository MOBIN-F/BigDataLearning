package com.mobin.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Created by hadoop on 4/2/16.
  */
object QueueStream {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("queueStream")
    val ssc = new StreamingContext(conf,Seconds(1))

    val rddQueue = new mutable.SynchronizedQueue[RDD[Int]]()

    val inputStream = ssc.queueStream(rddQueue)

    val mappedStream = inputStream.map(x => (x % 10,1))
    val reduceStream = mappedStream.reduceByKey(_ + _)
    reduceStream.print
    ssc.start()
    for(i <- 1 to 30){
      rddQueue += ssc.sparkContext.makeRDD(1 to 100, 2)
      Thread.sleep(1000)
    }

    ssc.stop()
  }
}
