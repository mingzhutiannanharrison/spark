package com.core.pairValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object subtract {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 8)
    val rdd2: RDD[Int] = sc.makeRDD(5 to 10)
    val unionRdd: RDD[Int] = rdd1.subtract(rdd2)
    // 在rdd1 中而不在rdd2 中的
    val str: String = unionRdd.sortBy(x=>x).collect().mkString(",")
    println(str)

  }
}
