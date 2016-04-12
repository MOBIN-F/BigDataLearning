package com.mobin.SparkRDDFun.TransFormation.KVRDD

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by hadoop on 4/11/16.
  */
object CombineByKey1 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("combinByKey")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(
      ("A", 3), ("A", 9), ("A", 12), ("A", 0), ("A", 5), ("B", 4),
      ("B", 10), ("B", 11), ("B", 20), ("B", 25), ("C", 32), ("C", 91),
      ("C", 122), ("C", 3), ("C", 55)), 2)

    val combineByKeyRDD = rdd.combineByKey(
      (x: Int) => (x, 1),
      (acc: (Int, Int), x) => (acc._1 + x, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    combineByKeyRDD.foreach(println)
    sc.stop()
  }
}
