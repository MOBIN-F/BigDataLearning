package com.mobin.Telecom

import java.text.SimpleDateFormat

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
    val statisticRdd = splicRdd.mapPartitions(statisticFun)
    statisticRdd.count()
  }

  def statisticFun(iter: Iterator[(String, (String,Int))]) = {
    var list = List[(String, String)]()
    while(iter.hasNext) {
      var sum = 0
      val data = iter.next()
      val str = data._2._1.split("\\|")
      for (m <- str) {
        val str1 = m.split(",")   //分隔出<时间，编号>
        if (str1.length == 2 && "0".equals(str1(1))) {
          for (n <- str) {
            val str2 = n.split(",")  //分隔出<时间， 编号>
            if (str2.length == 2 && ("1".equals(str2(1)) || "2".equals(str2(1)))) {
              val t1 =paresTime(str2(0))
              val t2 = paresTime(str1(0))
              if (!t1.isEmpty && !t2.isEmpty && t1.get - t2.get < 2000 && t1.get - t2.get > 0){
                sum = sum +1
              }
            }
          }
        }
      }
      println(data._1, sum + "," + data._2._2)
      list = (data._1, sum + "," + data._2._2) :: list
    }
    list.iterator
  }

  def paresTime(time: String) : Option[Long] = {
    val timeFormat = "yyyy-MM-dd HH:mm:ss SSS"
    val month = time.substring(5, 8)
    var t = time
    if (!month.contains("-")){
      month match {
        case "Jan" => t = t.replace(month, "01")
        case "Feb" => t = t.replace(month, "02")
        case "Mar" => t = t.replace(month, "03")
        case "Apr" => t = t.replace(month, "04")
        case "May" => t = t.replace(month, "05")
        case "Jun" => t = t.replace(month, "06")
        case "Jul" => t = t.replace(month, "07")
        case "Aug" => t = t.replace(month, "08")
        case "Sep" => t = t.replace(month, "09")
        case "Oct" => t = t.replace(month, "10")
        case "Nov" => t = t.replace(month, "11")
        case "Dec" => t = t.replace(month, "12")
        case _ => None
      }
    }
    var startTime: Option[Long] = None
    try {
        startTime = Some(new SimpleDateFormat(timeFormat).parse(time).getTime)
        return startTime
    }catch {
      case  e: Exception => None
    }
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
      val str = iter.next().split(",")
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
