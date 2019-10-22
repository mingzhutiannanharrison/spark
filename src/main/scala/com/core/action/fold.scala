package com.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object fold {
  def main(args: Array[String]): Unit = {

    /*
    * 1. 作用：折叠操作，aggregate的简化操作，seqop和combop一样。
      2. 需求：创建一个RDD，将所有元素相加得到结果
    * */

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(1 to 20)
    val num : Int = rdd.fold(1)(_+_)
    println(num)
  }
}
