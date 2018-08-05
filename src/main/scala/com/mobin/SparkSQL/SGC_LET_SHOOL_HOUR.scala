package com.mobin.SparkSQL

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Mobin on 2016/11/28.
  */
object SGC_LET_SHOOL_HOUR {

  case  class  School(school_name: String,  school_id: String, enodeb: Int)
  case  class  Mr(enodebID: Int, MSISDN: String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SGC_LET_SCHOOL_HOUR").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val school: DataFrame = sc.textFile("E:\\DATA\\PUBLIC\\NOCE\\school.csv").map(x => x.split("\\|")).map(s => School(s(1), s(3),Integer.parseInt(s(5)))).toDF()
    val mr = sc.textFile("F:\\2.10.txt").map(s => s.split("\\|")).map(mr => Mr(Integer.parseInt(mr(1)),mr(11))).toDF()
    school.registerTempTable("school")
    mr.registerTempTable("mr")
    school.select("school_name")
    val joinDF = school.join(mr,$"enodeb" === $"enodebID").select("school_id","school_name","MSISDN").distinct
    val countDF  = joinDF.select("school_id","school_name").groupBy("school_id","school_name")
    countDF.count().rdd.saveAsTextFile("F:\\SCHOOL.txt")

  }
}
