package com.mobin.SparkRDDFun.TransFormation.KVRDD

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by hadoop on 4/12/16.
  */
object Cogroup {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("ReduceByKey")
    val sc = new SparkContext(conf)
    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
    val rdd = sc.parallelize(arr, 3)
    val rdd1 = sc.parallelize(arr1, 3)
    val groupByKeyRDD = rdd.cogroup(rdd1)
    groupByKeyRDD.foreach(println)
    println(groupByKeyRDD.toDebugString)
    sc.stop
  }
}
