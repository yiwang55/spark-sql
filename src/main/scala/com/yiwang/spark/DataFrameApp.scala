package com.yiwang.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("DataFrameApp").getOrCreate()

    val peopleDF: DataFrame = spark.read.format("json").load("D:\\bigdata\\codefiles\\sparkSQL\\people.json")

    peopleDF.printSchema()

    //select top 20
    peopleDF.show()

    //select name from people
    peopleDF.select("name").show()

    //select name, age + 10 as age2 from people
    peopleDF.select(peopleDF.col("name"), (peopleDF.col("age") + 10).as("age2")).show()

    //select * from people where age > 19
    peopleDF.filter(peopleDF.col("age") > 19).show()

    //select age, count(*) from people group by age
    peopleDF.groupBy(peopleDF.col("age")).count().show()
    spark.close();
  }
}
