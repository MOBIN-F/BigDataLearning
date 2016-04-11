package com.mobin.SparkRDDFun.TransFormation.KVRDD

import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}

/**
  * Created by hadoop on 4/11/16.
  *
  */
object CombineByKey {

  def main(args: Array[String]) {
    /*
    def createCombine = (x: String) => (x, 1)
    def mergerValue = (peo: (String, Int), x: String) => (peo._1+","+x, peo._2 + 1)
    def mergeCombine = (sex1: (String, Int), sex2: (String, Int)) => (sex1._1 +","+ sex2._1, sex1._2 + sex2._2)*/
    val conf = new SparkConf().setMaster("local").setAppName("combinByKey")
    val sc = new SparkContext(conf)
    val people = List(("male", "Mobin"), ("male", "Kpop"), ("female", "Lucy"), ("male", "Lufei"), ("female", "Amy"))
    val rdd = sc.parallelize(people)
    val combinByKeyRDD = rdd.combineByKey(
      (x: String) => (x, 1),
      (peo: (String, Int), x: String) => (peo._1+","+x, peo._2 + 1),
      (sex1: (String, Int), sex2: (String, Int)) => (sex1._1 +","+ sex2._1, sex1._2 + sex2._2))

    combinByKeyRDD.foreach(println)
    sc.stop()
  }




}
