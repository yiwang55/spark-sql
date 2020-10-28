package com.yiwang.imooclog

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {
  //开始用的是simpledateformat 会出现日期解析错误的问题，因为它是线程不安全的！！
  //开始用的是simpledateformat 会出现日期解析错误的问题，因为它是线程不安全的！！
  private val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  private val TARGET_FORMAT = FastDateFormat.getInstance("YYYY-MM-dd HH:mm:ss")

  /**
   * @param time
   * @return
   */
  def getTime(time: String) = {
    try {
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime
    } catch {
      case e: Exception => {
        0L
      }
    }
  }

  /**
   * 转换时间
   *
   * @param time
   */
  def dateConvert(time: String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {
    println(dateConvert("[10/Nov/2016:00:01:02 +0800]"))
  }
}
