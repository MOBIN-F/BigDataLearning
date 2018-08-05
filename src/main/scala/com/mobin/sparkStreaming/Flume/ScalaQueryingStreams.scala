package com.mobin.sparkStreaming.Flume

import java.net.InetSocketAddress

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
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
object ScalaQueryingStreams {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Apache Log Transformer")
    val sparkContext = new SparkContext(conf)
    val streamCtx = new StreamingContext(sparkContext, Seconds(10))

    var address = new Array[InetSocketAddress](1)
    address(0) = new InetSocketAddress("localhost", 9998)
    val flumeStream = FlumeUtils.createPollingStream(streamCtx, address, StorageLevel.MEMORY_AND_DISK_SER_2, 1000, 1)
    val transformLog = new ScalaLogAnalyzerJson()
    val newDStream = flumeStream.map{
      x => transformLog.tansformLogDataIntoJson(new String(x.event.getBody.array()))
    }
    val wStream = newDStream.window(Seconds(40), Seconds(20))
    wStream.foreachRDD{
      rdd =>
        val sqlCtx = getInstance(sparkContext)
        //通过JSONRDD将 JSONRDD转换为SQL DataFrame
        val df = sqlCtx.jsonRDD(rdd)
        df.registerTempTable("apacheLogData")
        //打印结构类型
        df.printSchema()
        val logDataFrame = sqlCtx.sql("SELECT method,count(*) as total FROM apacheLogData GROUP BY method")
        logDataFrame.show()
    }

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

  @transient private var instance: SQLContext = null

  //延迟初始化SQLContext
  def getInstance(sparkContext: SparkContext): SQLContext =
    synchronized{
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
}
