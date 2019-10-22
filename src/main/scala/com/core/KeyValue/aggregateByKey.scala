package com.core.KeyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object aggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
    val sc = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
    val rdd2: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max(_,_),_+_)
    rdd2.collect().foreach(println)

  }
}
