package com.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object count {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.parallelize(6 to 10)
    val num: Long = rdd.count
    println(num)

  }
}
