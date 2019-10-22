package com.core.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark01_sample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(1 to 10)
    val res: RDD[Int] = rdd.sample(false,0.5,12L)
    val res1: Array[Int] = res.collect()
    for (elem <- res1) {
      println(elem)
    }
  }
}
