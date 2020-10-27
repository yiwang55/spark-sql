package com.yiwang.spark

import org.apache.spark.sql.SparkSession

object ParquetApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("ParquetApp").master("local[2]").getOrCreate()

    val df = sparkSession.read.parquet("D:\\bigdata\\codefiles\\sparkSQL\\users.parquet")
    df.printSchema()
  }
}
