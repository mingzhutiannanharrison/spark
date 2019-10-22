package com.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object takeOrdered {
  def main(args: Array[String]): Unit = {
    //作用：返回该RDD排序后的前n个元素组成的数组
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(12,34,1,4,7,3,10,12))
    val res: Array[Int] = rdd.takeOrdered(5)
    println(res.mkString(","))
  }
}
