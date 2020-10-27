package com.yiwang.spark

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DataSetApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("DataSetApp").master("local[2]").getOrCreate()
    val path = "D:\\bigdata\\codefiles\\sparkSQL\\sales.csv"
    val df: DataFrame = sparkSession.read.option("header", true).option("inferSchema", "true").csv(path)

    df.printSchema()
    //导入隐式转换
    import sparkSession.implicits._
    val ds: Dataset[Sales] = df.as[Sales]
    ds.map(e => e.itemid).show()

    sparkSession.close()
  }

  case class Sales(txid:Int, customeid:Int, itemid:Int, amount:Double)
}
