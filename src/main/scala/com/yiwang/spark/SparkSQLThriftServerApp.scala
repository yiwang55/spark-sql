package com.yiwang.spark

import java.sql.DriverManager

object SparkSQLThriftServerApp {
  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val conn = DriverManager.getConnection("jdbc:hive2://hadoop001:10000")
    val preparedStatement = conn.prepareStatement("select context from hive_wordcount")
    val resultSet = preparedStatement.executeQuery()
    while (resultSet.next()) {
      println("context: " + resultSet.getString("context"))
    }
    resultSet.close()
    preparedStatement.close()
    conn.close()
  }
}
