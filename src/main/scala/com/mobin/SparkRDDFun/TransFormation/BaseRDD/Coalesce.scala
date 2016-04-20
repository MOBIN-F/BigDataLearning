package com.mobin.SparkRDDFun.TransFormation.BaseRDD

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 4/9/16.
  * 对RDD的分区进行重新分区，shuffle默认值为false,当shuffle=false时，不能增加分区数
    目,但不会报错，只是分区个数还是原来的
  */
object Coalesce {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 16,4)
    rdd.foreachPartition(iter => print(iter.toList+ " | "))
    val coalesceRDD = rdd.coalesce(3)   //当suffle的值为false时，不能增加分区数(如分区数不能从5->7)
   // val coalesceRDD = rdd.coalesce(5,true)
    println("重新分区后的分区个数:"+coalesceRDD.partitions.size)
    println("RDD依赖关系:"+coalesceRDD.toDebugString)
    coalesceRDD.foreachPartition(iter => print(iter.toList+ " | "))
    sc.stop
  }
}
