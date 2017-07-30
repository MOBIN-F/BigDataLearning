package com.mobin.SparkRDDFun.TransFormation.KVRDD

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Mobin on 2017/7/30.
  */
object AggregateAndFold {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("AggregateFold")
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(1 to 10, 2)
    val rs = rdd1.aggregate(1)(
      (x,y) => x + y,
      (a,b) => a+ b
    )
    val rs1 = rdd1.fold(1)((x,y) => x+ y)
    println(rs)
    println(rs1)
  }
}
