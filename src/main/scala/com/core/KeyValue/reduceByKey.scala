package com.core.KeyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object reduceByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
    val sc = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.parallelize(List(("female",1),("male",5),("female",5),("male",2)))
    val rdd2: RDD[(String, Int)] = rdd.reduceByKey((x,y)=>x+y)
    rdd2.collect().foreach(println)
      }
}
