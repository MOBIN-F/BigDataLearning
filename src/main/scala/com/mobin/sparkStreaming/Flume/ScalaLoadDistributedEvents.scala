package com.mobin.sparkStreaming.Flume

import java.io.ObjectOutputStream
import java.net.InetSocketAddress

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created with IDEA
  * Creater: MOBIN
  * Date: 2018/8/2
  * Time: 3:22 PM
  */
object ScalaLoadDistributedEvents {

  def main(args: Array[String]): Unit = {
    println("Creating Spark Configuration")
    val conf = new SparkConf().setMaster("local[2]").setAppName("streaimg data loading App");
    println("Retreivinf Streaming Context from Spark Conf")
    val streamContext = new StreamingContext(conf, Seconds(2))

    //创建一个包含所有机器地址和端口的InetSocketAdress数组
    var address = new Array[InetSocketAddress](1)
    address(0) = new InetSocketAddress("localhost",9998)

    //创建一个Flume轮询流，每隔2s从Sink中拉取事件
    //1. maxBatchSize:单个RPC中从Spakr Sink中拉取事件的最大数目
    //2. 这个Stream发送给Sink的并发请求数目T
    val flumeStream = FlumeUtils.createPollingStream(streamContext, address, StorageLevel.MEMORY_AND_DISK_SER_2,1000, 1)

    val outputStream = new ObjectOutputStream(Console.out)
    printValues(flumeStream, streamContext, outputStream)
    streamContext.start()
    streamContext.awaitTermination()
  }


  def printValues(stream: DStream[SparkFlumeEvent], context: StreamingContext, outputStream: ObjectOutputStream): Unit ={
    stream.foreachRDD(foreachFunc)
    def foreachFunc = (rdd: RDD[SparkFlumeEvent]) => {
      val array = rdd.collect()
      println("Start Printing Results")
      println("Total size of Events = " + array.size)
      for (flumeEvent <- array){
        //从SparkFlumeEvent得到AvorFlumeEvent
        val payLoad = flumeEvent.event.getBody
        println(new String(payLoad.array()))
      }
      println("finish......")
    }
  }
}
