package com.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object reduce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(1 to 10)
    val sum: Int = rdd.reduce(_+_)
    println(sum)

    val rdd2 = sc.makeRDD(Array(("a",1),("a",3),("c",3),("d",5)))
    val tuple: (String, Int) = rdd2.reduce((x,y)=>(x._1 + y._1,x._2 + y._2))

    println(tuple)

  }
}
