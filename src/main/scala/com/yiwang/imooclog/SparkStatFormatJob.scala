package com.yiwang.imooclog

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()

    val logRDD: RDD[String] = spark.sparkContext.textFile("D:\\bigdata\\codefiles\\sparkSQL\\10000_access.log")
    logRDD.map(line => {
      val splits = line.split(" ")
      val ip = splits(0)
      //处理日期数据 format to YYYY-MM-dd HH:mm:ss
      val time = DateUtils.dateConvert(splits(3) + " " + splits(4))
      val url = splits(11).replaceAll("\"", "")
      //流量
      val flow = splits(9)

//      (ip, time, url, flow)
      time + "\t" + url + "\t" + flow + "\t" + ip
    }).saveAsTextFile("D:\\bigdata\\codefiles\\sparkSQL\\10000_access_output.log")

    spark.stop()
  }
}
