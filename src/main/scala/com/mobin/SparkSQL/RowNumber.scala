package com.mobin.SparkSQL

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Mobin on 2016/12/1.
  */
object RowNumber {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("rownum").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //  val dpiDF = sc.textFile("F:\\AGG_EVT_LTE_DPI_NEW.txt").map(x => x.split("\\|")).
    //      filter(x => x.length >= 30 &&  x(14).toDouble > 0 &&  x(15).toDouble > 0 && x(3) != "" && x(18) != "").
    //      map(x => DPI(x(3),x(18),x(14).toDouble,x(15).toDouble, x(14).toDouble+x(15).toDouble)).toDF()
    //
    //    dpiDF.registerTempTable("dpi")
    //    //dpiDF.groupBy("MDN").agg("size_ul" -> "sum","size_dl" -> "sum")
    //   // dpiDF.select("APP").groupBy("APP").count().select("count").show()
    // //  sqlContext.sql("SELECT MDN,APP,size_ul,size_dl,sum(s) FROM dpi").show()
    //    dpiDF.printSchema()
    //  //  val sDF = dpiDF.groupBy("MDN","APP").agg("s" -> "sum").registerTempTable("tmp")//每个用户对应的的APP的流量q
    //  sqlContext.sql("SELECT MDN,s,COUNT(1) FROM dpi GROUP BY s").show()
    // //   sDF.groupBy("MDN").agg("SUM(s)" -> "sum")
//    println("count.....")
//    val acc = sc.accumulator(0, "ac")
//    sc.textFile("/DATA/PUBLIC/NOCE/SGC/SGC_LTE_CDR_DAY/day=20161125/00*").foreach(
//      line => if(line.length > 0)  acc += 1
//    )
    //println("line:" + acc.value)

  //  dpiDF.show()
//    val mr = sc.textFile("E:\\DATA\\PUBLIC\\NOCE\\ETL\\ETL_4G_MRO_ERS\\20161020\\2016102011\\e_p_3_1.txt")
//      .map(x => (x.split("\\|")(3),2.10))
//      .filter(x => x!="").distinct().toDF()
//    val chr = sc.textFile("E:\\DATA\\PUBLIC\\NOCE\\AGG\\AGG_MRO_CHR_RELATE\\day=20161020\\hour=2016102011\\vendor=ERS\\10\\agg_data_172_17_1_2_ad7fc9ad_3930_4da8_97cc_a2a476f2333f.txt")
//      .map(x => (x.split("\\|")(1),1.8)).filter(x => x !="").distinct().toDF()
//
//    val rs = mr.unionAll(chr).count()
//    println(rs)
//    sc.stop()

    sc.textFile("F:\\m_p_50_3.txt.lzo").map(x => x.split(",")(0)).foreach(
      println(_)
    )

    sc.stop()

  }
}
