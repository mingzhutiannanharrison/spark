package com.core.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark01_distinct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(List(1,2,34,4,4,4))
    //val res: RDD[Int] = rdd.distinct()  //不指定并行度
    val res: RDD[Int] = rdd.distinct(3) //指定并行度
    for (elem <- res.collect()) {
      println(elem)
    }



  }
}
