package com.mobin.Telecom

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Mobin on 2017/7/15.
  * 飞行模式计算
  */
object AirPlaneMode {
  private val MSISDN = 6
  private val IDENTIFICATION = 8
  private val STARTTIME = 0
  private val ENB = 12
  private val MAX_NUM = 13
  private val chrSourcePath = "/DATA/PUBLIC/NOCE/ETL/ETL_CHR_L_MM/"

  def airPlainModeMain(fs: FileSystem, sc: SparkContext, day: String, dateTime :String, isPersist: Boolean): Unit ={
    val source = chrSourcePath + day + "/" + "*/*"
    print(source)
    val paresRdd = sc.textFile(source).mapPartitions(iterFunc)
    val splicRdd = paresRdd.reduceByKey(reduceByKeyFun)
  }

  def reduceByKeyFun(x1: (String, Int), x2: (String, Int)): (String,Int) = {
    val sum = x1._2 + x2._2
    if (",".equals(x1._1)){
      if (!",".equals(x2._1)){
          (x2._1, sum)
      } else {
        ("", sum)
      }
    } else {
      if (!",".equals(x2._1)) {
        (x1._1 + "|" + x2._1, sum)
      } else {
        (x1._1, sum)
      }
    }
  }


  def iterFunc(iter: Iterator[String]) = {
    var list = List[(String, (String, Int))]()
    while (iter.hasNext ) {
      val str= iter.next().toString.split(",")
      val enb:String = str(ENB)
      val mdn = str(MSISDN)
      val time = str(STARTTIME)
      val airplane = str(IDENTIFICATION)
      var tp = ""
      airplane match {
        case _ if "0x05".equals(airplane) =>  tp = time + ",0"
        case _ if "0x00".equals(airplane) || "0x18".equals(airplane) => tp = time + ",1"
        case _ => "" + ","
      }
      val enb_mdn = String.format("%s,%s", mdn, String.valueOf(Integer.parseInt(enb.substring(3), 16)))
       list = (enb_mdn, (tp, 1))::list
    }
    list.iterator
  }



  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("airPlainMode").setMaster("local")
    val sc = new SparkContext(conf)
    val configuration = new Configuration()
    val fs = FileSystem.newInstance(configuration)
    airPlainModeMain(fs, sc, "20170322", "", false)
  }
}
