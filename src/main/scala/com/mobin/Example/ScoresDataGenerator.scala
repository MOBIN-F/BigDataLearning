package com.mobin.Example

import java.io.FileWriter

import scala.util.Random

/**
  * Created by Mobin on 2016/12/22.
  * sno string, //学号
  * semester int, //学期
  * math int,   //  数学成绩
  * en int,  //     英语成绩
  * c int,   //     C语言成绩
  * os int   //   操作系统成绩

  */
object ScoresDataGenerator {
  private val FILE_OUTPATH = "Scores.txt"
  private val MAX_RECORD = 1000;

  def main(args: Array[String]) {
    Generator(FILE_OUTPATH,MAX_RECORD)
  }


  private def Generator(filePath: String, recordNum: Int) {
    var write: FileWriter = null
    try {
      write = new FileWriter(filePath, true)
      val rand = new Random()
      val term = 1
      for(i <- 1 to recordNum){
        val MScore = generatorScore
        val EScore = generatorScore
        val CScore = generatorScore
        val SScore = generatorScore
        write.write(i + "," + term + "," + MScore + "," + EScore + "," + CScore + "," + SScore)
        write.write(System.getProperty("line.separator"))
        write.flush()
      }
    } catch {
      case e => println("error")
    }finally {
      if (write != null)
         write.close()
    }
  }

  private def generatorScore: Int = {
    val rand = new Random()
    val sc = rand.nextInt(100)
    val score = sc match {
      case s if(s >0 &&  s <10) =>  s + 80
      case s if(s >10 &&  s < 30) =>  s + 70
      case s if(s >30 &&  s < 50) =>  s + 40
      case s if(s >50 &&  s < 60) =>  s + 20
      case _ => sc
    }
    score
  }

}
