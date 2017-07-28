package com.mobin.Example

/**
  * Created by Mobin on 2016/11/15.
  */
object AudienceAnalysis {

  lazy val nameIndexMap = {
    val nameIndexMap = scala.collection.mutable.HashMap.empty[String, Int]
    val basicNames = Seq("first_name", "last_name", "email", "company", "job", "street_address", "city",
    "state_abbr", "zipcode_plus4", "url", "phoen_number", "user_agent", "user_name")
    nameIndexMap ++= basicNames zip (0 to 12)
    for(i <- 0 to 328){
      nameIndexMap ++= Seq(("letter_" + i, i * 3 + 13),("number_" + i, i * 3 +14), ("bool_" + i, i *3 +15))
    }

    nameIndexMap
  }

  def $(name: String): Int = nameIndexMap.getOrElse(name, -1)
 }
