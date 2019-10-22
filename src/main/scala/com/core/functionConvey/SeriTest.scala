package com.core.functionConvey

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}



object SeriTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))
    val search = new search("hadoop")
    val match2: RDD[String] = search.getMatch2(rdd)
    match2.collect().foreach(println)
  }
}
