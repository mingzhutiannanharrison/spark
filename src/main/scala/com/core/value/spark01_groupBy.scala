package com.core.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark01_groupBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    val sc = new SparkContext(conf)
    //val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val rdd: RDD[Int] = sc.makeRDD(1 to 10)
    val res: RDD[(Int, Iterable[Int])] = rdd.groupBy(_%2)
    res.collect().foreach(println)
  }
}
