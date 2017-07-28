package com.mobin.SparkRDDFun.TransFormation.Action

import org.apache.spark.{SparkContext, SparkConf}

import scala.concurrent.Future

/**
  * Created by hadoop on 4/19/16.
  */
object KVFunc {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("KVFunc")
    val sc = new SparkContext(conf)
    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val rdd = sc.parallelize(arr,2)
    val countByKeyRDD = rdd.countByKey()
    val collectAsMapRDD = rdd.collectAsMap()
    val lookupRDD = rdd.lookup("A")
    println("countByKey:")
    countByKeyRDD.foreach(print)
    println("\ncollectAsMap:")
    collectAsMapRDD.foreach(print)
    println("\nlookup:")
    lookupRDD.foreach(x => print(x))
    sc.stop
  }
}
