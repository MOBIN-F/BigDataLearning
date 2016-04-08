package com.mobin.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hadoop on 3/31/16.
  */
object StateFull {

  def main(args: Array[String]) {
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val conf = new SparkConf().setMaster("local[2]").setAppName("stateFull")
    val sc = new StreamingContext(conf, Seconds(10))
    sc.checkpoint(".")

    val lines = sc.socketTextStream("master", 9998)
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1))

    val stateDstream = wordDstream.updateStateByKey[Int](updateFunc)
    stateDstream.print()
    sc.start()
    sc.awaitTermination()
  }
}
