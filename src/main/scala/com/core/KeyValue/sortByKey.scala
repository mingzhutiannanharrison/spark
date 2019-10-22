package com.core.KeyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object sortByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
    val sc = new SparkContext(conf)
    val rdd1: RDD[(Int, String)] = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
    val rdd2: RDD[(Int, String)] = rdd1.sortByKey(true)
    rdd2.collect().foreach(println)
    val rdd3: RDD[(Int, String)] = rdd1.sortByKey(false)
    rdd3.collect().foreach(println)
  }
}
