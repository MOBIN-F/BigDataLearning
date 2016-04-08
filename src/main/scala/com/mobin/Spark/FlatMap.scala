package com.mobin.Spark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by hadoop on 4/7/16.
  */
object FlatMap {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("flatmap")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 5)
    val fm = rdd.flatMap(x => (1 to x))
    fm.foreach( x => print(x + " "))
    sc.stop()
  }
}
