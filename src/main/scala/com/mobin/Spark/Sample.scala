package com.mobin.Spark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by hadoop on 4/7/16.
  */
object Sample {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 10)
    val sample1 = rdd.sample(true,0.5,0)
    sample1.collect.foreach(x => print(x + " "))
    sc.stop
  }

}
