package com.mobin.Spark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by hadoop on 4/7/16.
  */
object Map {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 10)  //创建RDD
    val map = rdd.map(_*2)       //对RDD中的每个元素都乘于2
    map.foreach(x => print(x+" "))
    sc.stop()
  }
}
