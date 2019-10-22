package com.SQL

import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

object avgAge_udaf_class{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("avgAge_udaf")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    var udaf = new avgAge_udaf_class

    //用这种方式定义的聚合函数只能用DSL风格的语法来写
    val avgCol: TypedColumn[userBean, Double] = udaf.toColumn.name("avgAge")
    val frame: DataFrame = spark.read.json("data/people.json")
    val userDS: Dataset[userBean] = frame.as[userBean]
    userDS.select(avgCol).show()
  }
}

case class userBean(age:BigInt,name:String)
case class avgBuffer( var sum:BigInt, var count:Int)
class avgAge_udaf_class extends  Aggregator [userBean,avgBuffer,Double]{
  //初始化
  override def zero: avgBuffer = {
    avgBuffer(0,0)
  }

  //单个节点中的更新操作
  override def reduce(b: avgBuffer, a: userBean): avgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count+1
    b
  }

  //各节点合并操作
  override def merge(b1: avgBuffer, b2: avgBuffer): avgBuffer = {
    avgBuffer(b1.sum + b2.sum,b1.count+b2.count)
  }

  //计算操作
  override def finish(reduction: avgBuffer): Double = {
    reduction.sum.toDouble/reduction.count
  }

  //下面2个是固定的写法
  override def bufferEncoder: Encoder[avgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}