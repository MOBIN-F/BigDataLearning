package com.mobin.Advanced_Analytics_with_Spark

import org.apache.spark.util.StatCounter
/**
  * Created by Mobin on 2017/3/8.
  */
class NaStatCounter extends  Serializable{

  val  stats: StatCounter = new StatCounter()
  var missing: Long = 0

  def add(x: Double): NaStatCounter = {
    if(java.lang.Double.isNaN(x)){
      missing += 1
    } else {
      stats.merge(x)
    }
    this
  }

  def merge(other: NaStatCounter): NaStatCounter = {
    stats.merge(other.stats)
    missing += other.missing
    this
  }

  override def toString = {
    "stats: " + stats.toString()  + "NaN: " + missing
  }
}

object NaStatCounter extends  Serializable{
  def apply(x: Double) = new NaStatCounter().add(x )
}
