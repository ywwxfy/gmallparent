package com.asiainfo.gmall.publisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * 监控业务数据库的变化，订单表，用户表
 * 实时计算交易额
 */
public interface OrderMapper {
    //获取每天日活总数
    public Double getOrderAmountTotal(String dayString);
    //分时统计数目，拿到当天每小时的数据
    public List<Map> getOrderAmountHourTotal(String dayString);
}
