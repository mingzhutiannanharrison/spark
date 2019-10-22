package com.core.value

import org.apache.spark.{SparkConf, SparkContext}

object spark01_makeRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd learn")
    //val conf = new SparkConf().setAppName("WC")
    val sc = new SparkContext(conf)
    //val listRDD = sc.makeRDD(List(1,24,4,5,6))
    //listRDD.collect().foreach(println)

    val hdfsRDD = sc.textFile("hdfs://master:9000/test/input")
    hdfsRDD.collect().foreach(println)
    val tuples = hdfsRDD.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,false).collect()
    tuples.foreach(println)
  }

}
