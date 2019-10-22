package com.streaming.streamming

import org.apache.log4j.lf5.LogLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RDDStream {
  def main(args: Array[String]): Unit = {

    //1）需求：循环创建几个RDD，将RDD放入队列。通过SparkStream创建Dstream，计算WordCount
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("filestream")
    val sc = new SparkContext(conf)
    sc.setLogLevel(LogLevel.WARN.toString)
    val ssc = new StreamingContext(sc, Seconds(5))

    //创建rdd队列
    val rddQueue = new mutable.Queue[RDD[Int]]()

    //创建QueueInputDstream
    val InputStream: InputDStream[Int] = ssc.queueStream(rddQueue,oneAtATime = false)
    val dStream: DStream[(Int, Int)] = InputStream.map((_,1)).reduceByKey(_+_)
    dStream.print()
    ssc.start()
    for(i<-1 to 5){
      rddQueue += ssc.sparkContext.makeRDD(1 to 300,10)
      Thread.sleep(20)
    }
    ssc.awaitTermination()
  }
}
