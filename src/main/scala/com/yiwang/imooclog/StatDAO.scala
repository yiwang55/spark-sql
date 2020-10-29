package com.yiwang.imooclog

import java.sql.{Connection, PreparedStatement}
import com.imooc.log.MySQLUtils
import scala.collection.mutable.ListBuffer

/**
 * 各个维度统计的DAO操作
 */
object StatDAO {
  def insertDayVideoAccessTopN(list: ListBuffer[DayVideoAccessStat])={
    var connection: Connection = null
    var pstmt:PreparedStatement = null
    try{
      connection = MySQLUtils.getConnection()

      connection.setAutoCommit(false)//设置手动提交

      pstmt = connection.prepareStatement(
        "insert into day_video_access_topn_stat(day,cms_id,times)" +
          "values(?, ?, ?)")
      for (elem <- list) {
        pstmt.setString(1, elem.day)
        pstmt.setLong(2, elem.cmsId)
        pstmt.setLong(3, elem.times)

        pstmt.addBatch()
      }
      pstmt.executeBatch()//执行批量处理，性能会好很多
      connection.commit()//手工提交

    }catch {
      case exception: Exception => exception.printStackTrace()
    }finally {
      MySQLUtils.release(connection, pstmt)
    }

  }
  def insertDayCityVideoAccessTopN(list: ListBuffer[DayCityVideoAccessStat])={
    var connection: Connection = null
    var pstmt:PreparedStatement = null
    try{
      connection = MySQLUtils.getConnection()

      connection.setAutoCommit(false)//设置手动提交

      pstmt = connection.prepareStatement(
        "insert into day_video_city_access_topn_stat(day,cms_id,city,times, times_rank)" +
          "values(?, ?, ?, ?, ?)")
      for (elem <- list) {
        pstmt.setString(1, elem.day)
        pstmt.setLong(2, elem.cmsId)
        pstmt.setString(3, elem.city)
        pstmt.setLong(4, elem.times)
        pstmt.setLong(5, elem.times_rank)

        pstmt.addBatch()
      }
      pstmt.executeBatch()//执行批量处理，性能会好很多
      connection.commit()//手工提交

    }catch {
      case exception: Exception => exception.printStackTrace()
    }finally {
      MySQLUtils.release(connection, pstmt)
    }

  }
}
