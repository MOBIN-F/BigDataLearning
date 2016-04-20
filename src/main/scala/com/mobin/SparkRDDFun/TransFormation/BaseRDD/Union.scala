package com.mobin.SparkRDDFun.TransFormation.BaseRDD

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 4/8/16.
  * :将两个RDD中的数据集进行合并，最终返回两个RDD的并集，若RDD中存在相同的元素也不会去重
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
