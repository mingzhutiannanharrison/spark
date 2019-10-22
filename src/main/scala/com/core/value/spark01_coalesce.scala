package com.core.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark01_coalesce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    val sc = new SparkContext(conf)
    //改变分区个数
    val rdd: RDD[Int] = sc.parallelize(1 to 20,5)
    println(rdd.partitions.size)

    val rdd2: RDD[Int] = rdd.coalesce(3)
    println(rdd2.partitions.size)


  }
}
