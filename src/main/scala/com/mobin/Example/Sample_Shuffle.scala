package com.mobin.Example

import java.util

import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

/**
  * Created by Mobin on 2017/8/30.
  *方案实现思路：(Spark性能优化指南——高级篇[美团点评技术博客])
1.对包含少数几个数据量过大的key的那个RDD，通过sample算子采样出一份样本来，然后统计一下每个key的数量，计算出来数据量最大的是哪几个key。

2.然后将这几个key对应的数据从原来的RDD中拆分出来，形成一个单独的RDD，并给每个key都打上n以内的随机数作为前缀，而不会导致倾斜的大部分key形成另外一个RDD。

3.接着将需要join的另一个RDD，也过滤出来那几个倾斜key对应的数据并形成一个单独的RDD，将每条数据膨胀成n条数据，这n条数据都按顺序附加一个0~n的前缀，不会导致倾斜的大部分key也形成另外一个RDD。

4.再将附加了随机前缀的独立RDD与另一个膨胀n倍的独立RDD进行join，此时就可以将原先相同的key打散成n份，分散到多个task中去进行join了。

5.而另外两个普通的RDD就照常join即可。

6.最后将两次join的结果使用union算子合并起来即可，就是最终的join结果。
  */
object Sample_Shuffle {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sample")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("SampleJoin1.txt").map(x => {
      val kv = x.split(",")
      (kv(0), kv(1))
    })
    //对rdd1进行采样
    val sampleRdd = rdd1.sample(false, 0.1)
    //统计出各key的频数
    val countSampleRdd = sampleRdd.map(x =>(x._1, 1)).reduceByKey(_ + _)
    val reversedSampleRdd = countSampleRdd.map(x => (x._2, x._1))
    //对频数进行排序,得到频数最高的对应的key
    val skewedUserid = reversedSampleRdd.sortByKey(false).take(1)(0)._2
    //从RDD1中拆分出导致数据倾斜的key，形成独立的RDD
    val skewRdd = rdd1.filter(_._1.equals(skewedUserid))
    ////从RDD1中拆分出不会导致数据倾斜的key，形成独立的RDD
    val commonRdd = rdd1.filter(!_._1.equals(skewedUserid))

    val rdd2 = sc.textFile("SampleJoin2.txt").map(x => {
      val kv = x.split(",")
      (kv(0), kv(1))
    })

    println("skew: " + skewedUserid)
    //对RDD2中skew key扩充100倍
    val skewRdd2 = rdd2.filter(_._1.equals(skewedUserid)).flatMap(x => {
      for(i <- 1 to 10)yield((i + "_" + x._1, x._2))
    })

    //为skewRdd中的每条数据都打上随机前缀并join上dkewRdd2
    val joinRdd = skewRdd.map(x=>{
      val prefix = Random.nextInt(10)
      (prefix + "_" + x._1, x._2)
    }).join(skewRdd2).map(x => {
      val key = x._1.split("_")(1)
      (key,x._2)
    })

    val joinRdd2 = commonRdd.join(rdd2)
    val resultRdd = joinRdd.union(joinRdd2)
    resultRdd.foreach(println)
  }
}
