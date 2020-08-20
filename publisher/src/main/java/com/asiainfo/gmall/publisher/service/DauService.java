package com.asiainfo.gmall.publisher.service;

import com.asiainfo.gmall.publisher.mapper.DauMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;


public interface DauService {
    public Long getDauTotal(String dayString);
    public Map<String,Long> getDauHourTotal(String dayString);
    public Double getOrderAmountTotal(String dayString);
    public Map<String,Double> getOrderAmountHourTotal(String dayString);

    //给一个时间和skuname关键词
    public Map getSaleDetail(String dayString, String skuKeyword,int pagesize,int pageNum);

}
