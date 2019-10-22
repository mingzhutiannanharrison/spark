package com.core.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark01_repartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.parallelize(1 to 20,4)
    println(rdd.partitions.size)

    //底层是调用coalesce
    val rdd2: RDD[Int] = rdd.repartition(3)

    println(rdd2.partitions.size)

  }
}
