package com.yiwang.imooclog

import org.apache.spark.sql.SparkSession

object TopNStatJob {
  val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()
}
