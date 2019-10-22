package com.streaming.streamming

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(1 to 10 ,2)
      print(rdd.collect().mkString(","))

  }
}
