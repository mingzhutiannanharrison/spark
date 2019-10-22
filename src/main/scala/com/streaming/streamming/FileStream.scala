package com.streaming.streamming

import org.apache.log4j.lf5.LogLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileStream {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("filestream")
    val sc = new SparkContext(conf)
    sc.setLogLevel(LogLevel.WARN.toString)
    val ssc = new StreamingContext(sc,Seconds(5))


    val dirStream: DStream[String] = ssc.textFileStream("hdfs://master:9000/fileStream")
    val wordAndCountStreams: DStream[(String, Int)] = dirStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    wordAndCountStreams.print()
    ssc.start()
    ssc.awaitTermination()
    
  }
}
