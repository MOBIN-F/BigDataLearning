package com.mobin.SparkRDDFun.TransFormation.RDDBase

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 4/8/16.
  */
object Union {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(1 to 4)
    val rdd2 = sc.parallelize(3 to 5)
    val unionRDD = rdd1.intersection(rdd2) //union  intersection
    unionRDD.collect.foreach(x => print(x + " "))
    sc.stop()
  }
}
