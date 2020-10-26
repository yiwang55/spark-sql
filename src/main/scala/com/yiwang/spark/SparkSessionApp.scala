package com.yiwang.spark

import org.apache
import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

object SparkSessionApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = new apache.spark.sql.SparkSession
    .Builder().appName("SparkSessionApp").master("local[2]")
      .getOrCreate()
    val frame: DataFrame = sparkSession.read.json("D:\\bigdata\\codefiles\\sparkSQL\\people.json")
    frame.show();

    sparkSession.close()
  }
}
