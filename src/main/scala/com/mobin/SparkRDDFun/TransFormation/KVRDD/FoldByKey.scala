package com.mobin.SparkRDDFun.TransFormation.KVRDD

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by hadoop on 4/11/16.
  */
object FoldByKey {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("FoldByKey")
    val sc = new SparkContext(conf)
    val people = List(("Mobin", 2), ("Mobin", 1), ("Lucy", 2), ("Amy", 1), ("Lucy", 3))
    val rdd = sc.parallelize(people)
    val foldByKeyRDD = rdd.foldByKey(2)(_ + _)
    foldByKeyRDD.foreach(println)
    sc.stop
  }
}
