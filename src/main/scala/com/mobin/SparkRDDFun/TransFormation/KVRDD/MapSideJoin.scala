package com.mobin.SparkRDDFun.TransFormation.KVRDD

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Mobin on 2016/11/14.
  */
object MapSideJoin {

  def main(args: Array[String]) {
    val  conf = new SparkConf().setMaster("local[2]").setAppName("Mobin")
    val sc = new SparkContext(conf)
    val table = sc.textFile("mapjoin.txt")
    val table1 = sc.textFile("mapjoin1.txt")
    val paisr = table.map{ x =>
      var pos = x.indexOf(",")
      (x.substring(0,pos),x.substring(pos+1))
    }.collectAsMap()

    //    var broadcastMap = sc.broadcast(paisr)

    val result = table1.map{ x =>
      var pos = x.indexOf(",")
      (x.substring(0,pos),x.substring(pos + 1))
    }.mapPartitions({ iter =>
      //  var m = broadcastMap.value
      for {
        (key, value) <- iter
        if paisr.contains(key)
      }yield(key,(value , paisr.get(key).getOrElse("")))
    })

    result.saveAsTextFile("result")
  }
}
