package com.core.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark01_filter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.makeRDD(Array("harrison","harrisonabc","aliceabc"))
    val array: Array[String] = rdd.collect().filter(_.contains("harrison"))
    array.foreach(println)

  }
}
