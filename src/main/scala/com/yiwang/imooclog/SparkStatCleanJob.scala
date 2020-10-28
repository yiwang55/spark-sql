package com.yiwang.imooclog

import org.apache.spark.sql.SparkSession

/**
 * use spark to clean data
 */
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()

    val logRDD1 = spark.sparkContext.textFile("D:\\bigdata\\codefiles\\sparkSQL\\10000_access_output.log\\part-00000")
    val logRDD2 = spark.sparkContext.textFile("D:\\bigdata\\codefiles\\sparkSQL\\10000_access_output.log\\part-00001")
    val logRDD = logRDD1.union(logRDD2)
    logRDD.take(10).foreach(println)

//    val dataFrame = spark.read.format("parquet").load(
//      "D:\\bigdata\\codefiles\\sparkSQL\\10000_access_output.log\\part-00000"
//      , "D:\\bigdata\\codefiles\\sparkSQL\\10000_access_output.log\\part-00001"
//    )
//    dataFrame.printSchema()

    spark.stop()
  }
}
