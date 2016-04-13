package com.mobin.HDFS

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by hadoop on 3/6/16.
  * 为文件中的第一个字段拼接一个随机值再写入HDFS
  */
object WriteToHDFS {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: WriteToHDFS <InputPath> <OutputPath>\n")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("sogou")
    val sc = new SparkContext(conf)
    val sgfile = sc.textFile(args(0))

    val rdd = sgfile.map(lines => {
      val line = lines.split("\t")
      if(line.length == 6){
        val one = line(0) +"-"+ new Random().nextInt()
        (one, line(1),line(2),line(3),line(4),line(5))
      }

    })
    rdd.saveAsTextFile(args(1))
    sc.stop()

  }
}
