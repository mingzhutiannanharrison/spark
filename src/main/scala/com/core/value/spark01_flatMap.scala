package com.core.value

import org.apache.spark.{SparkConf, SparkContext}

object spark01_flatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(List(1,2),List(3,4),List(5,6)))
    val res = rdd.flatMap(x=>x)
    res.foreach(println)

    val rdd2 = sc.makeRDD(1 to 5)
    val res2 = rdd2.flatMap( 0 to _).collect()
    res2.foreach(println)
  }
}
