package com.asiainfo.gmall.realtime.bean

import java.util

/**
  * test git 标识
  * @param mid
  * @param uids
  * @param itemids
  * @param events
  * @param ts
  */
case class AlertInfo(
                    mid: String,
                    uids:util.HashSet[String],
                    itemids:util.HashSet[String],
                    events:util.ArrayList[String],
                    ts:Long
)