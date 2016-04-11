package com.mobin.SparkRDDFun.TransFormation.RDDBase

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 4/7/16.
  * 与map类似，但每个元素输入项都可以被映射到0个或多个的输出项，最终将结果”扁平化“后输出
  */
object FlatMap {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("flatmap")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 5)
    val fm = rdd.flatMap(x => (1 to x))
    fm.foreach( x => print(x + " "))
    sc.stop()
  }
}
