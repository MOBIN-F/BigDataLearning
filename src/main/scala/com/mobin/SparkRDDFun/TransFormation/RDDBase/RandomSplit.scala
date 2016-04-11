package com.mobin.SparkRDDFun.TransFormation.RDDBase

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 4/9/16.
  * 根据weight权重值将一个RDD划分成多个RDD,权重越高划分得到的元素较多的几率就越大
  */
object RandomSplit {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 10)
    val randomSplitRDD = rdd.randomSplit(Array(1.0,2.0,7.0))
    randomSplitRDD(0).foreach(x => print(x +" gg"))
    randomSplitRDD(1).foreach(x => print(x +" rr"))
    randomSplitRDD(2).foreach(x => print(x +" tt"))
    sc.stop
  }
}
