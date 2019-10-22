package com.core.pairValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object cartesian {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 3)
    val rdd2: RDD[Int] = sc.makeRDD(5 to 7)
    //求笛卡尔积
    val res: RDD[(Int, Int)] = rdd1.cartesian(rdd2)
    val tuples: Array[(Int, Int)] = res.collect()
    for (elem <- tuples) {
      println(elem)
    }


  }
}
