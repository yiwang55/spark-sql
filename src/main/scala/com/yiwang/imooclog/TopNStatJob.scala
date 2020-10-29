package com.yiwang.imooclog

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object TopNStatJob {

  def cityAccessTopNStat(spark: SparkSession, logDF: DataFrame) = {
    /**
     * DataFrame API方式统计
     */
    import spark.implicits._
    val result = logDF.filter($"day" === "20170511" && $"cmsType" === "video")
      .groupBy("day", "city", "cmsId")
      .agg(count("cmsId").as("times"))
    //      .show(false)
    val tableResult = result.select(
      result("day"),
      result("city"),
      result("cmsId"),
      result("times"),
      //相当于数据库row_number()over(partition by xx order by xxx)函数
      row_number().over(Window.partitionBy(result("city"))
        .orderBy(result("times").desc)).as("times_rank")
    ).filter("times_rank <= 3")

    tableResult.foreachPartition(patitionOfRecords => {
      val list = new ListBuffer[DayCityVideoAccessStat]

      patitionOfRecords.foreach(e => {
        val day = e.getAs[String]("day")
        val cmsId = e.getAs[Long]("cmsId")
        val city = e.getAs[String]("city")
        val times = e.getAs[Long]("times")
        val times_rank = e.getAs[Int]("times_rank")

        list.append(DayCityVideoAccessStat(day, cmsId, city, times, times_rank))
      })

      StatDAO.insertDayCityVideoAccessTopN(list)
    })

  }

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
    try {
      logDF.createOrReplaceTempView("access_logs")
      val result = spark.sql("select " +
        "day,cmsId,count(cmsId) as times   " +
        "from access_logs " +
        "where day = '20170511' and cmsType='video'  " +
        "group by day,cmsId " +
        "order by times desc")
      //      result.show(false)
      result.foreachPartition(patitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]

        patitionOfRecords.foreach(e => {
          val day = e.getAs[String]("day")
          val cmsId = e.getAs[Long]("cmsId")
          val times = e.getAs[Long]("times")

          list.append(DayVideoAccessStat(day, cmsId, times))
        })

        StatDAO.insertDayVideoAccessTopN(list)
      })
    } catch {
      case exception: Exception => exception.printStackTrace()
    } finally {
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TopNStatJob")
      .master("local[2]")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .getOrCreate()

    val logDF = spark.read.format("parquet").load("D:\\bigdata\\codefiles\\sparkSQL\\format_cleaned.log")

//        logDF.printSchema()
//        logDF.show(10, false)

    //最受欢迎的topN课程
    //    videoAccessTopNStat(spark, logDF)

    //按照地市来统计topN课程
    cityAccessTopNStat(spark, logDF)
    spark.stop()
  }
}
