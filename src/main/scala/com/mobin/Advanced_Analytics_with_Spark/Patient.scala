package com.mobin.Advanced_Analytics_with_Spark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Mobin on 2017/3/7.
  */
case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)
object Patient {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("Patient")
    val sc = new SparkContext(conf)
    val rawblocks = sc.textFile(args(0))
    val mds = rawblocks.filter(!isHeader(_)).map(pares)
   // val grouped = mds.groupBy(x => x.matched).mapValues(x => x.size).foreach(println)  //按matched分组统计
   // val sort = mds.map(x => x.matched).sortBy(_).foreach(println)
    val nsdRDD = mds.map(md =>
     md.scores.map(d => NaStatCounter(d))
   ).foreach(x => println(x(1)))
  }

  def isHeader(line: String): Boolean = {
    line.contains("id_1")
  }

  def toDouble(s: String): Double = {
    if ("?".equals(s))
      Double.NaN
    else
      s.toDouble
  }

  def  pares(line: String)={
    val pieces = line.split(",")
    val id1 = pieces(0).toInt
    val id2 = pieces(1).toInt
    val scores = pieces.slice(2,11).map(toDouble)  //取数据的[2,11)位并转化成Double类型
    val matched = pieces(11).toBoolean
    MatchData(id1, id2, scores, matched)
  }
}
