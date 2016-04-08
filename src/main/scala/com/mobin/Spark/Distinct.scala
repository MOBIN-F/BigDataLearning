package com.mobin.Spark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by hadoop on 4/8/16.
  */
object Distinct {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    val list = List(1,1,2,5,2,9,6,1)
    val distinctRDD = sc.parallelize(list)
    val unionRDD = distinctRDD.distinct() //union   intersection
    unionRDD.collect.foreach(x => print(x + " "))
    sc.stop()
  }
}
