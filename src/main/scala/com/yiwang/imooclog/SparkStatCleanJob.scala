package com.yiwang.imooclog

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
 * use spark to clean data
 */
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()

    //load files
    val logRDD = spark.sparkContext.textFile("D:\\bigdata\\codefiles\\sparkSQL\\format.log")
    //mapping log string to RDD[Row]
    val value: RDD[Row] = logRDD.map(line => LogConvertUtil.parseLog(line))
    //create logDateFrame
    val logDF = spark.createDataFrame(value, LogConvertUtil.struct)
    logDF.printSchema()
    logDF.show(10,false)

    spark.stop()
  }
}
