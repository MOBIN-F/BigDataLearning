package com.mobin.SparkRDDFun.TransFormation.BaseRDD

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Mobin on 2017/7/29.
  */
object ZipWithIndex {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("ZipWithIndex")
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(Seq("A","B","C","D","E","F"),2)
    rdd1.zipWithIndex().foreach(println)
    rdd1.zipWithUniqueId().foreach(println)
  }
}
