package com.core.KeyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object cogroup {
  def main(args: Array[String]): Unit = {
    /*
    * 1. 作用：在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
      2. 需求：创建两个pairRDD，并将key相同的数据聚合到一个迭代器。
    *
    * */
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
    val sc = new SparkContext(conf)
    val rdd1: RDD[(Int, String)] = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
    val rdd2: RDD[(Int, Int)] = sc.parallelize(Array((1,4),(2,5),(3,6)))
    rdd1.cogroup(rdd2).collect().foreach(println)
  }
}
