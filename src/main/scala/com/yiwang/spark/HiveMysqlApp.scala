package com.yiwang.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveMysqlApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("HiveMysqlApp").getOrCreate()

    val hiveDF = spark.table("hive_wordcount")

//    import java.util.Properties
//    val connectionProperties = new Properties()
//    connectionProperties.put("user", "root")
//    connectionProperties.put("password", "root")
//    connectionProperties.put("driver", "com.mysql.jdbc.Driver")
//
//    val mysqlDF = spark.read.jdbc("jdbc:mysql://hadoop001:3306", "test.DEPT", connectionProperties)

    //另一种写法
    val mysqlDF = spark.read.format("jdbc").option("url", "jdbc:mysql://hadoop001:3306/test").option("dbtable", "DEPT").option("user", "root").option("password", "root").option("driver", "com.mysql.jdbc.Driver").load()

    val joinDF = hiveDF.join(mysqlDF, hiveDF("context")=== mysqlDF("DEPTNO"))
    joinDF.printSchema();
  }
}
