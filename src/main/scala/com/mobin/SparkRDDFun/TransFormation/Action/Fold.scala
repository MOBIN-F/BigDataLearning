package com.mobin.SparkRDDFun.TransFormation.Action

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by hadoop on 4/20/16.
  * 通过op函数聚合各分区中的元素及合并各分区的元素，op函数需要两个参数，在开始时第一个传入的参数为zeroValue,T为RDD数据集的数据类型
  */
object Fold {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("Fold")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array(("a", 1), ("b", 2), ("a", 2), ("c", 5), ("a", 3)), 2)
    val foldRDD = rdd.fold(("d", 0))((val1, val2) => { if (val1._2 >= val2._2) val1 else val2
    })
    println(foldRDD)
  }

  /**
  1.开始时将(“d”,0)作为op函数的第一个参数传入，将Array中和第一个元素("a",1)作为op函数的第二个参数传入，并比较value的值，
    返回value值较大的元素

  2.将上一步返回的元素又作为op函数的第一个参数传入，Array的下一个元素作为op函数的第二个参数传入，比较大小

  3.重复第2步骤
    */
}
