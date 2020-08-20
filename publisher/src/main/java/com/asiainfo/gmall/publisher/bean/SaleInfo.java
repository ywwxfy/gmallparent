package com.asiainfo.gmall.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Map;

//封装最后结果，转化成json字符串返回
@Data
@AllArgsConstructor
public class SaleInfo {
    private Long total;
    private List<Option> rateList;
    private  List<Map> saleList;
}
