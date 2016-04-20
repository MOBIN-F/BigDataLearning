package com.mobin.SparkRDDFun.TransFormation.Action

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by hadoop on 4/20/16.
  * seqOp函数将每个分区的数据聚合成类型为U的值，comOp函数将各分区的U类型数据聚合起来得到类型为U的值
  */
object Aggregate {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("Fold")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1,2,3,4),2)
    val aggregateRDD = rdd.aggregate(2)(_+_,_ * _)
    println(aggregateRDD)
    sc.stop
  }

  /**
    * 步骤1：分区1：zeroValue+1+2=5   分区2：zeroValue+3+4=9

     步骤2：2*分区1的结果*分区2的结果=90
    */

}
