package com.core.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/*
*
* 样例数据
* 1516609143867 6 7 64 16
* 1516609143869 9 4 75 18
* 1516609143869 1 7 87 12
*
*
* */



object Practice {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
    val sc = new SparkContext(conf)
    val line: RDD[String] = sc.textFile("/Users/jiaxionghu/Documents/大数据资料/032_Spark/2.资料/agent.log")

    val provinceAdToOne: RDD[((String, String), Int)] = line.map {
      x =>
        val fields: Array[String] = x.split(" ");
        ((fields(4), fields(1)), 1)
    }
    val provinceAdToSum: RDD[((String, String), Int)] = provinceAdToOne.reduceByKey(_+_)

    val provinceToAdSum : RDD[(String, (String, Int))] = provinceAdToSum.map((x => ((x._1._1), (x._1._2, x._2))))

    val provinceGroup: RDD[(String, Iterable[(String, Int)])] = provinceToAdSum.groupByKey()


    val provinceAdTop3: RDD[(String, List[(String, Int)])] = provinceGroup.mapValues{x=>x.toList.sortWith((x,y)=>(x._2 > y._2)).take(3)}
    provinceAdTop3.collect().foreach(println)

    sc.stop()
  }

}
