package com.mobin.sparkStreaming.Flume

import java.util.regex.{Matcher, Pattern}

import com.alibaba.fastjson.JSON






/**
  * Created with IDEA
  * Creater: MOBIN
  * Date: 2018/8/4
  * Time: 2:36 PM
  */
class ScalaLogAnalyzerJson extends Serializable {

  def tansformLogDataIntoJson(logLine: String): String = {
    val LOG_ENTRY_PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+)"""
    val PATTERN = Pattern.compile(LOG_ENTRY_PATTERN)
    val matcher = PATTERN.matcher(logLine)

    if (!matcher.find()){
      println("Cannot parse logline" + logLine)
    }
    import scala.collection.JavaConversions._
    val json = scala.util.parsing.json.JSONObject(createDataMap(matcher)).toString()
    println(json)
    return json
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
