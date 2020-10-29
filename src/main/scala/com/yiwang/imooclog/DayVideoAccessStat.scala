package com.yiwang.imooclog

/**
 * 每天课程访问次数实体类
 */
case class DayVideoAccessStat(day: String, cmsId: Long, times: Long)
case class DayVideoFlowsAccessStat(day: String, cmsId: Long, flows: Long)
case class DayCityVideoAccessStat(day: String, cmsId: Long,city:String, times: Long, times_rank:Int)
