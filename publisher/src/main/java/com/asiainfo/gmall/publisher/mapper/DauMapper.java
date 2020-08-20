package com.asiainfo.gmall.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    //获取每天日活总数
    public Long getCount(String dayString);
    //分时统计数目，拿到当天每小时的数据
    public List<Map> getHourTotalCount(String dayString);
}
