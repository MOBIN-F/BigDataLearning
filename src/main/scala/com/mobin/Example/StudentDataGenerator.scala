package com.mobin.Example

import java.io.FileWriter

import scala.util.Random

/**
  * Created by Mobin on 2016/12/22.
  * sno string, //学号
  * name string,//姓名
  * sex string, //性别
  * age int,    //年龄
  * class string //班级
  */
object StudentDataGenerator {
  private val FILE_OUTPATH = "Student.txt"
  private val MAX_RECORD = 10000;

  def main(args: Array[String]) {
    Generator(FILE_OUTPATH, MAX_RECORD)
  }

  private def Generator(filePath: String, recordNum: Int) {

    var write: FileWriter = null
    try {
      write = new FileWriter(filePath, true)
      val rand = new Random();
      for (i <- 1 to recordNum) {
        val name = nameGenerator
        val sex = sexGenerator
        //年龄在20~22之间
        val age = rand.nextInt(3) + 20
        //班级
        val classNum = rand.nextInt(6)
        write.write(i + "," + name + "," + sex + "," + age + "," + classNum)
        write.write(System.getProperty("line.separator"))
        write.flush()
      }
      } catch {
        case e => println("error")
      } finally {
        if (write != null)
          write.close()
      }
  }

  //生成姓名
  private def nameGenerator: String = {
    val higthPos = (176 + Math.abs(new Random().nextInt(39)))
    val lowPos = (176 + Math.abs(new Random().nextInt(93)))
    val name = Array[Byte](new Integer(higthPos).byteValue(), new Integer(lowPos).byteValue())
    val surname = Array("钟", "李", "张", "刘", "王", "章", "洪", "江", "戴")
    surname(new Random().nextInt(9)) + new String(name, "GBK")
  }

  //生成性别
  private def sexGenerator: String = {
    val random = new Random()
    val randomNum = random.nextInt(2) + 1
    randomNum % 2 match {
      case 0 => "男"
      case _ => "女"
    }
  }
}


