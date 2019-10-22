package com.SQL

import org.apache.spark.sql.{DataFrame, SparkSession}

object udf {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("udf")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.read.json("data/people.json")

    spark.udf.register("addName",(x:String)=>"Name: " + x)

    df.createOrReplaceTempView("people")

    spark.sql("select addName(name) from people").show()


    spark.close()

  }
}
