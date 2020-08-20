package com.asiainfo.gmall.publisher.service;

import java.util.Map;


public interface OrderAmountService {
    public Double getOrderAmountTotal(String dayString);
    public Map<String,Double> getOrderAmountHourTotal(String dayString);

}
