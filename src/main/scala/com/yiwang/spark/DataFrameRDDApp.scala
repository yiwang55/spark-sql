package com.yiwang.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * DataFrame和RDD互操作的两种方式：
 * 1）反射：case class   前提：事先需要知道你的字段、字段类型
 * 2）编程：Row          如果第一种情况不能满足你的要求（事先不知道列）
 * 3) 选型：优先考虑第一种
 */
object DataFrameRDDApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[2]").appName("DataFrameRDDApp").getOrCreate()

//    infoReflection(sparkSession)
    program(sparkSession)
    sparkSession.close()
  }

  def program(sparkSession: SparkSession) = {
    val rdd: RDD[String] = sparkSession.sparkContext.textFile("D:\\bigdata\\codefiles\\sparkSQL\\infos.txt")
    val rowRdd: RDD[Row] = rdd.map(_.split(",")).map(e => Row(e(0).toInt, e(1), e(2).toInt))

    val structType: StructType = StructType(Array(StructField("id", IntegerType, true)
      , StructField("name", StringType, true)
      , StructField("age", IntegerType, true)))

    val infoDF: DataFrame = sparkSession.createDataFrame(rowRdd, structType)

    infoDF.printSchema()
    infoDF.show()

  }

  def infoReflection(sparkSession: SparkSession) = {
    val infoRDD: RDD[String] = sparkSession.sparkContext.textFile("D:\\bigdata\\codefiles\\sparkSQL\\infos.txt")
    //
    import sparkSession.implicits._
    val info: DataFrame = infoRDD.map(_.split(","))
      .map(e => Info(e(0).toInt, e(1), e(2).toInt))
      .toDF
    info.show()
    info.filter(info.col("age") > 30).show
    info.createOrReplaceTempView("infos")
    sparkSession.sql("select * from infos where age >30").show
  }

  case class Info(id:Int, name:String, age:Int)
}
