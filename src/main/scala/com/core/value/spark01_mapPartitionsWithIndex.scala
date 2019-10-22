package com.core.value

import org.apache.spark.{SparkConf, SparkContext}

object spark01_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("mapPartitionsWithIndex")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array(1,2,3,4,5,6,7))
    //val res = rdd.mapPartitionsWithIndex((index,item)=>item.map((index,_)))
    val res = rdd.mapPartitionsWithIndex((index,items)=>(items.map((index,_))))
    res.foreach(println)
  }
}
