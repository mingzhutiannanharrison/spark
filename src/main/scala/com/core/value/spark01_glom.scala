package com.core.value

import org.apache.spark.{SparkConf, SparkContext}

object spark01_glom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("glom")
    val sc = new SparkContext(conf)

    //将每个分区中的数放到数组中
    val rdd = sc.parallelize(1 to 16,5)
//    val res = rdd.glom().collect();
//    res.foreach(println)
    rdd.glom().collect().foreach(array=>{
      //println(array.mkString(","))
      for (elem <- array) {
        println(elem)
      }
    })


  }
}
