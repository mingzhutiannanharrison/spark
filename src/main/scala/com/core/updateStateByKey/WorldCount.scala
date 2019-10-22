package com.core.updateStateByKey

import org.apache.log4j.lf5.LogLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object WorldCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","work")
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("update by key ")
    val sc = new SparkContext(conf)
    sc.setLogLevel(LogLevel.WARN.toString)
    sc.setCheckpointDir("hdfs://master:9000/checkpoint")

    val ssc = new StreamingContext(sc,Seconds(5))

    val   updateFunc=(values: Seq[Int],state: Option[Int])=>{
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.0.135",9999)

    //val res: DStream[(String, Int)] = inputDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    val dStream: DStream[(String, Int)] = inputDStream.flatMap(_.split(" ")).map((_,1))
    val res: DStream[(String, Int)] = dStream.updateStateByKey(updateFunc)
    res.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
