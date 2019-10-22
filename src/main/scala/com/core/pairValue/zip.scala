package com.core.pairValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object zip {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("zip")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.makeRDD(Array(1,2,3))
    val rdd2: RDD[String] = sc.makeRDD(Array("a","b","c"))
    val rdd3: RDD[(Int, String)] = rdd1.zip(rdd2)
    val tuples: Array[(Int, String)] = rdd3.collect()
    tuples.foreach(println)

    val rdd4: RDD[(String, Int)] = rdd2.zip(rdd1)
    rdd4.collect().foreach(println)
  }
}
