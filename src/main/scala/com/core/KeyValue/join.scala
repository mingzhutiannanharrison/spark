package com.core.KeyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object join {
  def main(args: Array[String]): Unit = {
    //创建两个pairRDD，并将key相同的数据聚合到一个元组。
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
    val sc = new SparkContext(conf)
    val rdd1: RDD[(Int, String)] = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
    val rdd2: RDD[(Int, Int)] = sc.parallelize(Array((1,4),(2,5),(3,6)))
    val tuples: Array[(Int, (String, Int))] = rdd1.join(rdd2).collect()
    tuples.foreach(println)

  }
}
