package com.core.value

import org.apache.spark.{SparkConf, SparkContext}

object spark01_map {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("map rdd")
    val sc = new SparkContext(conf)
    val arrRDD = sc.parallelize(1 to 30)
    val changedRdd = arrRDD.map(_*2)
    changedRdd.foreach(println)
  }
}
