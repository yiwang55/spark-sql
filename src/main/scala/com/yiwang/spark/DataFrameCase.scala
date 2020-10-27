package com.yiwang.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameCase {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("DataFrameCase").master("local[2]").getOrCreate()

    val rdd: RDD[String] = sparkSession.sparkContext.textFile("D:\\bigdata\\codefiles\\sparkSQL\\student.data")
    import sparkSession.implicits._
    val dataFrame = rdd.map(_.split("\\|")).map(e => Student(e(0).toInt, e(1), e(2), e(3))).toDF()

    dataFrame.printSchema()
//    dataFrame.show
//    dataFrame.show(25)
//    dataFrame.show(25, false)
//    dataFrame.select("name", "email").show(25, false)
//    dataFrame.filter("name ='' or name='NULL' ").show
//    dataFrame.filter("substring(name, 0, 1) = 'C'").show
//    dataFrame.sort(dataFrame("id")).show
//    dataFrame.sort(dataFrame("id").desc).show
    dataFrame.sort(dataFrame("name").asc, dataFrame("id").desc).show(25)

    val dataFrame2 = rdd.map(_.split("\\|")).map(e => Student(e(0).toInt, e(1), e(2), e(3))).toDF()

    val joinedFrame: DataFrame = dataFrame.join(dataFrame2, dataFrame("id") === dataFrame2("id"))

    //org.apache.spark.sql.AnalysisException: Reference 'id' is ambiguous, could be: id#5, id#27.;
    //你可以使用的时候指定你要用哪个df里面的字段
    joinedFrame.select(dataFrame("id").as("one_id"), dataFrame2("id")).show()

    sparkSession.close()
  }
  case class Student(id: Int, name:String, phone:String, email:String)
}
