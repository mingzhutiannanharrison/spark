package com.core.KeyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object mapValues {
  def main(args: Array[String]): Unit = {
    //创建一个pairRDD，并将value添加字符串"|||"
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
    val sc = new SparkContext(conf)
    val rdd1: RDD[(Int, String)] = sc.parallelize(Array((1,"a"),(1,"d"),(2,"b"),(3,"c")))
    val rdd2: RDD[(Int, String)] = rdd1.mapValues(_+"_harrison")
    rdd2.collect().foreach(print)
  }
}
