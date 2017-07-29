package com.mobin.SparkRDDFun.TransFormation.KVRDD

import org.apache.spark.{Partitioner, HashPartitioner, SparkContext, SparkConf}
import scala.collection.mutable.{Map}

/**
  * Created by hadoop on 4/10/16.
  */
object PartitionBy {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("partitonby")
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(Array((10,"A"), (20, "B"), (30,"C"), (40,"D")), 2)
    rdd1.mapPartitionsWithIndex{
      (partitionID, iter) => {
        var partiton_map = Map[String, List[(Int, String)]]()
        while(iter.hasNext){
          val partition_name = "part_" + partitionID
          var elem = iter.next()
          if (partiton_map.contains(partition_name)){
            var elems = partiton_map(partition_name)
            elem :: elems
          }else {
            partiton_map(partition_name) = List[(Int, String)]{elem}
          }
        }
        partiton_map.iterator
      }
    }
    rdd1.foreach(println)
    val rdd2 = rdd1.partitionBy(new HashPartitioner(2))
    var rdd3 = rdd1.groupByKey(new Partitioner() {
      override def numPartitions: Int = 10

      override def getPartition(key: Any): Int = {

        val id = key.asInstanceOf[Int]
        println(id)
        if (id % 2 ==0) {
              id / 4
        }else{
           id % 4
        }
      }
    })
    rdd3.foreach(println)
  }
}
