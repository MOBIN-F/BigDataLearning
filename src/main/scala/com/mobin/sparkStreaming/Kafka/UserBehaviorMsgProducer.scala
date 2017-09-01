package com.mobin.sparkStreaming.Kafka
import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.util.Random

/**
  * Created by Mobin on 2017/9/1.
  * 生产行为数据消息
  */
class UserBehaviorMsgProducer(brokers: String, topic: String) extends Runnable{
  private val brokerList = brokers
  private val targetTopic = topic
  private val props = new Properties()
  props.put("metadata.broker.list", brokerList)
  props.put("producer.type", "async")
  private val config = new ProducerConfig(props)
  private val producer = new Producer[String, String](config)

  private val  PAGE_NUM =100
  private val MAX_MSG_NUM= 3
  private val MAX_CLICK_TIME = 5
  private val MAX_STAY_TIME = 10
  private val LIKE_OR_NOT = Array[Int](1, 0, -1)


  override def run(): Unit = {
    val rand = new Random()
    while (true) {
      val msgNum = rand.nextInt(MAX_MSG_NUM) + 1
      for (i <- msgNum) {
        val msg = new StringBuffer()
        msg.append("page" + (rand.nextInt(PAGE_NUM) + 1))
        msg.append("|")
        msg.append(rand.nextInt(MAX_CLICK_TIME) + 1)
        msg.append("|")
        msg.append(rand.nextInt(MAX_CLICK_TIME) + rand.nextFloat())
        msg.append("|")
        msg.append(LIKE_OR_NOT(rand.nextInt(3)))
        println(msg.toString)
        sendMessage(msg.toString)
      }
      println("%d user behavior message producer.".format(msgNum + 1))
    }
  }

  def sendMessage(message: String) = {
    try{
      val data = new KeyedMessage[String, String](topic, message)
      producer.send(data)
    }catch {
      case e: Exception => println(e)
    }
  }

  object UserBehaviorMsgProducerClient{
    def main(args: Array[String]) {
      if (args.length < 2 ){
        println("Usage: UserBehaviorMsgProducerClient ip:9092 user-behavior-topic")
        System.exit(1)
      }
      new Thread(new UserBehaviorMsgProducer(args(0), args(1))).start()
    }
  }
}
