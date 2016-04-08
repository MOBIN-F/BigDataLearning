package com.mobin.sparkStreaming

import java.io.PrintWriter
import java.net.ServerSocket

/**
  * Created by hadoop on 3/28/16.
  */
object GenerateChar {
  def generateContext(index : Int) : String = {
    import scala.collection.mutable.ListBuffer
    val charList = ListBuffer[Char]()
    for(i <- 65 to 90)
      charList += i.toChar

    val charArray = charList.toArray
    charArray(index).toString
  }

  def index = {
    import java.util.Random
    val rdm = new Random
    rdm.nextInt(20)
  }

  def main(args: Array[String]) {
    val listener = new ServerSocket(9998)
    println("开始监听...............")
    while(true){
      val socket = listener.accept()
      new Thread(){
        override def run() = {
          println("Got client connected from :"+ socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream,true)
          while(true){
            Thread.sleep(500)
            val context = generateContext(index)
            println(context)
            out.write(context + '\n')
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}
