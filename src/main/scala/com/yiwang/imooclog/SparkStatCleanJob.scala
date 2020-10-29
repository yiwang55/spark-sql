package com.yiwang.imooclog

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

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
//    logDF.printSchema()
//    logDF.show(10,false)
    //用day分区写文件
    logDF
      .coalesce(1)//指定输出文件个数，避免小文件太多。影响性能
      .write.format("parquet")
      .mode(SaveMode.Overwrite)//覆盖模式，如果存在即覆盖
      .partitionBy("day")//分区可以传一个或多个
      .save("D:\\bigdata\\codefiles\\sparkSQL\\format_cleaned.log")

    spark.stop()
  }
}
