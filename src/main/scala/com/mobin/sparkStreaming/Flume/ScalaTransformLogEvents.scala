package com.mobin.sparkStreaming.Flume

import java.net.InetSocketAddress

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created with IDEA
  * Creater: MOBIN
  * Date: 2018/8/4
  * Time: 2:49 PM
  */
object ScalaTransformLogEvents {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Apache Log Transformer")
    val streamCtx = new StreamingContext(conf, Seconds(10))

    var address = new Array[InetSocketAddress](1)
    address(0) = new InetSocketAddress("localhost", 9998)
    val flumeStream = FlumeUtils.createPollingStream(streamCtx, address, StorageLevel.MEMORY_AND_DISK_SER_2, 1000, 1)
    val transformLog = new ScalaLogAnalyzerMap()
    val newDStream = flumeStream.flatMap{

      x => transformLog.tansformLogData(new String(x.event.getBody.array()))
    }

    println("------")
    flumeStream.map(x => x.event.getHeaders).print()
    println("------")


    executeTransformations(newDStream, streamCtx)
    streamCtx.start()
    streamCtx.awaitTermination()
  }

  def executeTransformations(dstream: DStream[(String, String)], context: StreamingContext): Unit ={
    printLogValues(dstream,context)
    println("++++++")
    dstream.filter(x => x._1.equals("method") && x._2.contains("GET")).count().print()
    println("++++++")

  }

  def printLogValues(stream: DStream[(String, String)], context: StreamingContext){
    stream.foreachRDD(foreachFunc)

    def foreachFunc = (rdd: RDD[(String,String)]) => {
      val array = rdd.collect()
      for (dataMap <- array.array){
        println(dataMap._1 + "------" + dataMap._2)
      }
    }
  }
}
