package com.mobin.SparkRDDFun.TransFormation.RDDBase

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 4/8/16.
  * 对RDD中的元素进行去重
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
