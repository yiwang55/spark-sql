package com.yiwang.spark

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

object HiveContextApp{
  def main(args: Array[String]): Unit = {
    val hiveContext = new HiveContext(new SparkContext(new SparkConf()
      .setMaster("local[*]")
      .setAppName("HiveContextApp")))
    hiveContext.table("hive_wordcount").show()
  }
}
