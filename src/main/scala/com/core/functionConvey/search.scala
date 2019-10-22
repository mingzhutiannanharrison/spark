package com.core.functionConvey
import org.apache.spark.rdd.RDD

class search(s:String) extends  Serializable {
  //过滤出包含字符串的数据
  var query=None
  def isMatch(s:String):Boolean  ={
    s.contains(query)
  }

  //过滤出包含字符串的rdd
  def getMatch(rdd:RDD[String]):RDD[String]={
    rdd.filter(isMatch)
  }

  def getMatch2(rdd:RDD[String]):RDD[String]={
    rdd.filter(x=>x.contains(s))
  }
}

