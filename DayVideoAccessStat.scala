package com.imooc.log

/**
 * 每天访问次数实体类
 * @param day
 * @param cmsId
 * @param times
 */

case class DayVideoAccessStat(day: String, cmsId: Long, times: Long)
