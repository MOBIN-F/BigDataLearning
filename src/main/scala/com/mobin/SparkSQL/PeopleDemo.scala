package com.mobin.SparkSQL

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Mobin on 2016/11/28.
  */
object PeopleDemo {
  def main(args: Array[String]) {
    val conf =new SparkConf().setAppName("people").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.jsonFile("people.json")
    df.show()
    df.printSchema()
    printf("select name------")
   df.select("name").show()
  }
}
