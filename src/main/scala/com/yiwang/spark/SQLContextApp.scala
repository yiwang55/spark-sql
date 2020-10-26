package com.yiwang.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * SQLContext的使用
 */
object SQLContextApp {
  def main(args: Array[String]): Unit = {
    val path = args(0)

    val sqlContext = new SQLContext(new SparkContext(new SparkConf().setMaster("local[*]").setAppName("SQLContextApp")))

    val people: DataFrame = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show();
  }
}
