package com.yiwang.spark

import org.apache.spark.sql.SparkSession

object ParquetApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("ParquetApp").master("local[2]").getOrCreate()

//    val df = sparkSession.read.parquet("D:\\bigdata\\codefiles\\sparkSQL\\users.parquet")
//    val df = sparkSession.read.format("parquet").load("D:\\bigdata\\codefiles\\sparkSQL\\users.parquet")
    val df = sparkSession.read.format("parquet").option("path", "D:\\bigdata\\codefiles\\sparkSQL\\users.parquet").load()
    df.printSchema()
    df.show

    df.select("name","favorite_color").show
//    df.select("name","favorite_color").write.format("json")
//        .save("D:\\bigdata\\codefiles\\sparkSQL\\users_out.json")
    df.select("name","favorite_color").write.format("json")
      .option("path", "D:\\bigdata\\codefiles\\sparkSQL\\users_out_1.json").save()


    sparkSession.stop()
  }
}
