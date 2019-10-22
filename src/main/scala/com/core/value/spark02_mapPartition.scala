package com.core.value

import org.apache.spark.{SparkConf, SparkContext}

object spark02_mapPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("mapPartiton")
    val sc = new SparkContext(conf)
    val a = sc.parallelize(1 to 10,3)


    def myFunctionPerEle(e:Int):Int={
      println("e=" + e)
      e*2
    }

    def myFunctionPerPartition(iter:Iterator[Int]):Iterator[Int]={
      println("run in partition")
      val res =for(e <- iter) yield e*2
      res
    }

    val b = a.map(myFunctionPerEle).collect

    val c = a.mapPartitions(myFunctionPerPartition).collect()


  }
}
