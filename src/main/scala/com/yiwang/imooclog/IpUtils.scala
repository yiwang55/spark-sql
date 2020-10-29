package com.yiwang.imooclog

import com.ggstar.util.ip.IpHelper

object IpUtils {
  def getCity(ip:String)={
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    print(getCity("218.75.35.226"))
  }

}
