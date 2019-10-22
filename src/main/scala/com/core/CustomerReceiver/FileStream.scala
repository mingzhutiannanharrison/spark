package com.core.CustomerReceiver

import org.apache.log4j.lf5.LogLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}


object FileStream {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FileStream")
    val sc = new SparkContext(conf)
    sc.setLogLevel(LogLevel.WARN.toString)
    val ssc = new StreamingContext(sc,Seconds(5))

    val receiverInputStream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("192.168.0.135",9999))

    val res: DStream[(String, Int)] = receiverInputStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    res.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
