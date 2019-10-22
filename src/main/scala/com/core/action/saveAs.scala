package com.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object saveAs {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 10)
    //rdd.saveAsTextFile("./savefile")
    rdd.saveAsObjectFile("./saveAsObject")
  }

}
