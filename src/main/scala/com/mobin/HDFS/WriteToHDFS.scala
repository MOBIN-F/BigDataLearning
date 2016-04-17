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
    val conf = new SparkConf().setMaster("local").setAppName("WriteToHDFS")
    val sc = new SparkContext(conf)
    val sgfile = sc.textFile(args(0))

    val rdd = sgfile.map(lines => {
      val line = lines.split("\\s")
      if(line.length == 6){
        val one = line(0) +"-"+ new Random().nextInt()
        one+","+line(1)+","+line(2).getBytes+","+line(3)+","+line(4)+","+line(5)
      }else   //如果这样写  一定不能只写if语句  还要加上else语句，否则没有通过if的，将被视了() 否则后期通过Phoenix导入到HBase中会因为字段不合法而报错
        "mobin1"+","+"mobin2"+","+"mobin3"+"mobin4"+","+"mobin5"+","+"mobin6"
    })
    rdd.saveAsTextFile(args(1))
    sc.stop()
  }
}
