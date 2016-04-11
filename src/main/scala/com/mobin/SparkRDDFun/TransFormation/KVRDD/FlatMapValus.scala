package com.mobin.SparkRDDFun.TransFormation.KVRDD

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by hadoop on 4/10/16.
  * 对[K,V]型数据中的V值flatmap操作
  */
object FlatMapValus {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    val list = List(("mobin",22),("kpop",20),("lufei",23))
    val rdd = sc.parallelize(list)
    val mapValuesRDD = rdd.flatMapValues(x => Seq(x,"male"))
    mapValuesRDD.foreach(println)
  }
}
