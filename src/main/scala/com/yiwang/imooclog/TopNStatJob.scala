package com.yiwang.imooclog

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object TopNStatJob {

  def videoAccessTopNStat(spark: SparkSession, logDF: DataFrame) = {
    /**
     * DataFrame API方式统计
     */
    //    import spark.implicits._
//    val result = logDF.filter($"day"==="20170511" && $"cmsType"==="video")
//      .groupBy("day","cmsId")
//      .agg(count("cmsId").as("times"))
//      .orderBy($"times".desc)

    /**
     * spark sql方式统计
     */
    logDF.createOrReplaceTempView("access_logs")
    val result = spark.sql("select " +
      "day,cmsId,count(cmsId) as times   " +
      "from access_logs " +
      "where day = '20170511' and cmsType='video'  " +
      "group by day,cmsId " +
      "order by times desc")
    result.show(false)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TopNStatJob")
      .master("local[2]")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled","false")
      .getOrCreate()

    val logDF = spark.read.format("parquet").load("D:\\bigdata\\codefiles\\sparkSQL\\format_cleaned.log")

//    logDF.printSchema()
//    logDF.show(10, false)

    videoAccessTopNStat(spark, logDF)

    spark.stop()
  }
}
