package com.mobin.SparkRDDFun.TransFormation.BaseRDD

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Mobin on 2017/7/29.
  */
object MapPartitionsWithIndex {

  def mappartitionWithIndexFun(x : Int, iter :Iterator[Int])={
    var result = List[String]()
    var i = 0
    while (iter.hasNext) {
      i += iter.next()
    }
    result.::(x + "|" + i).iterator
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("mappartitionsWithIndex")
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(1 to 5,2)
    val rdd2 = rdd1.mapPartitionsWithIndex{
      (x, iter) => {
        var result = List[String]()
        var i = 0
        while (iter.hasNext){
          i += iter.next()
        }
        result.::(x + "|" + i).iterator
      }
    }
    val rdd3 = rdd1.mapPartitionsWithIndex(mappartitionWithIndexFun)
    rdd3.foreach(println)
  }
}
