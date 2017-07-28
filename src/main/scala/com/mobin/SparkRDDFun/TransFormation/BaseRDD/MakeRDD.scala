package com.mobin.SparkRDDFun.TransFormation.BaseRDD

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Mobin on 2017/7/28.
  */
object MakeRDD {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("makeRDD")
    val sc = new SparkContext(conf)
    val collection = Seq((1 to 10, Seq("master","slave1")),
      (11 to 15, Seq("slave2","slave3")))
    var rdd = sc.makeRDD(collection)
    println(rdd.partitions.size)
    println(rdd.preferredLocations(rdd.partitions(0)))
  }
}
