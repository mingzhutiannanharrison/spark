package com.core.KeyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object fordByKey {
  def main(args: Array[String]): Unit = {
//    1.作用：aggregateByKey的简化操作，seqop和combop相同
//    2.需求：创建一个pairRDD，计算相同key对应值的相加结果

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
    val sc = new SparkContext(conf)
    val rdd: RDD[(Int, Int)] = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
    val resRDD: RDD[(Int, Int)] = rdd.foldByKey(0)(_+_).sortBy(_._1)
    resRDD.collect().foreach(println)
  }
}
