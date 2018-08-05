package com.mobin.sparkStreaming.Flume

import java.util.regex.{Matcher, Pattern}



/**
  * Created with IDEA
  * Creater: MOBIN
  * Date: 2018/8/4
  * Time: 2:36 PM
  */
class ScalaLogAnalyzerMap extends Serializable {

  def tansformLogData(logLine: String): Map[String, String] = {
    val LOG_ENTRY_PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+)"""
    val PATTERN = Pattern.compile(LOG_ENTRY_PATTERN)
    val matcher = PATTERN.matcher(logLine)

    if (!matcher.find()){
      println("Cannot parse logline" + logLine)
    }
    createDataMap(matcher)
  }

  def createDataMap(matcher: Matcher): Map[String, String] = {
    Map[String, String](
      ("IP" -> matcher.group(1)),
      ("client" -> matcher.group(2)),
      ("user" -> matcher.group(3)),
      ("date" -> matcher.group(4)),
      ("method" -> matcher.group(5)),
      ("request" -> matcher.group(6)),
      ("protocol" -> matcher.group(7)),
      ("respCode" -> matcher.group(8)),
      ("size" -> matcher.group(9))
    )
  }

}
