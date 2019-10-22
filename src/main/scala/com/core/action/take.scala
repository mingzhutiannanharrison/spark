package com.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object take {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(Array(2,3,6,7,4,5))

    val arr: Array[Int] = rdd.take(3)

    arr.foreach(println)
    println(arr.mkString(","))
  }

}
