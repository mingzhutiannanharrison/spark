package com.core.pairValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object intersection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 8)
    val rdd2: RDD[Int] = sc.makeRDD(5 to 10)
    //求交集
    val unionRdd: RDD[Int] = rdd1.intersection(rdd2)
    val str: String = unionRdd.sortBy(x=>x).collect().mkString(",")
    println(str)

  }
}
