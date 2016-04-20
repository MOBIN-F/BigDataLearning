package com.mobin.SparkRDDFun.TransFormation.BaseRDD

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 4/8/16.
  * 对两个RDD中的所有元素进行笛卡尔积操作
  */
object Cartesian {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(1 to 3)
    val rdd2 = sc.parallelize(2 to 5)
    val cartesianRDD = rdd1.cartesian(rdd2)

    cartesianRDD.foreach(x => println(x + " "))
  }
}
