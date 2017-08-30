package com.mobin.Example

import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

/**
  * Created by Mobin on 2017/8/29.
  * 先局部聚合再全局聚合
  */
object RandomPrefix_Shuffle {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RandomPrefix")
    val sc = new SparkContext(conf)
    val line = sc.textFile("RandomPrefix.txt").map((_,1))
    val randomPrefixRdd = line.map(x => {
      val random = Random
      val prefix = random.nextInt(10)
      (prefix + "_" + x._1  , x._2)
    })

    val localAggrRdd = randomPrefixRdd.reduceByKey(_ + _)
    val removeRandPrefixRdd = localAggrRdd.map(x => {
      val k = x._1.split("_")(1)
      (k, x._2)
    })
    val globalAggrRdd = removeRandPrefixRdd.reduceByKey(_ + _)
  }
}
