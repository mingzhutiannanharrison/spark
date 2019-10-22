package com.core.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark01_sortby {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,54,2,4,8,34,51,42))
    val rdd2: RDD[Int] = rdd.sortBy(x=>0-x)
    println(rdd2.collect().mkString(","))

  }
}
