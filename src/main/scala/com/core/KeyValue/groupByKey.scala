package com.core.KeyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object groupByKey {
  /*
  * 1. 作用：groupByKey也是对每个key进行操作，但只生成一个sequence。
  * 2. 需求：创建一个pairRDD，将相同key对应值聚合到一个sequence中，并计算相同key对应值的相加结果。
  * */
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
    val sc = new SparkContext(conf)
    val arrs: Array[String] = Array("one", "two", "two", "three", "three", "three")

    val rdd: RDD[(String, Int)] = sc.makeRDD(arrs).map(word=>(word,1))
    val res: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    res.collect().foreach(println)

    val res1: RDD[(String, Int)] = res.map(t=>(t._1,t._2.sum))
    res1.collect().foreach(println)

  }
}
