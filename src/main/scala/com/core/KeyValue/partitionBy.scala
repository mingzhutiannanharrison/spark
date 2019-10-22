package com.core.KeyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object partitionBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("zip")
    val sc = new SparkContext(conf)
    val rdd: RDD[(Int, String)] = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)
    println(rdd.partitions.size)
    val rdd2: RDD[(Int, String)] = rdd.partitionBy(new HashPartitioner(2))
    println(rdd2.partitions.size)
  }
}
