package com.mobin.Example

import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by Mobin on 2016/12/22.
  */
object SparkJoin {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkJoin").setMaster("local")
    val sc = new SparkContext(conf)
    val student = sc.textFile("Student.t")
    val scores = sc.textFile("Scores.txt")
    val studentT = student.map(str => str.split(",")).map(x => (x(0), x(1) +"," + x(2) + "," +x(3) + "," + x(4)))
    val scoresT = scores.map(str => str.split(",")).map(x => (x(0), x(1) +"," + x(2) + "," +x(3) + "," + x(4) + "," + x(5)))
    studentT.join(scoresT).foreach(println)
  }
}
