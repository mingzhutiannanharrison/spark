package com.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object countByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
    val sc = new SparkContext(conf)
    val rdd: RDD[(Int, Int)] = sc.makeRDD(Array((3,1),(4,1),(5,1),(6,1),(6,2)))
    val res: collection.Map[Int, Long] = rdd.countByKey()
    res.foreach(println)
  }
}
