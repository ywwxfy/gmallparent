package com.asiainfo.gmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.asiainfo.gmall.publisher.bean.Option;
import com.asiainfo.gmall.publisher.bean.OptionGroup;
import com.asiainfo.gmall.publisher.bean.SaleInfo;
import com.asiainfo.gmall.publisher.service.DauService;
import com.asiainfo.gmall.publisher.service.OrderAmountService;
import com.sun.tools.jdi.EventSetImpl;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import sun.tools.jstat.OptionLister;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class DauController {
    @Autowired
    DauService dauService;

    @RequestMapping("realtime-total")
    public String getDauCountJson(@RequestParam("date") String dayString) {
        //日活总数	[{"id":"dau","name":"新增日活","value":1200},
        //新增设备数 {"id":"new_mid","name":"新增设备","value":233}]
        List<Map> list = new ArrayList<>();
        Long dauTotal = dauService.getDauTotal(dayString);
        Map map = new HashMap<>();
        map.put("id", "dau");
        map.put("name", "新增日活");
        if (dauTotal==null){
            map.put("value", 0);
        }else{
            map.put("value", dauTotal);

        }
        list.add(map);
        //新增设备数
        map = new HashMap<>();
        map.put("id", "new_add");
        map.put("name", "新增设备数");
        map.put("value", new Random().nextInt(100));
        list.add(map);
        //新增交易额
        Double orderAmountTotal = dauService.getOrderAmountTotal(dayString);
        map = new HashMap<>();
        map.put("id", "order_amount");
        map.put("name", "新增交易额");
        if(orderAmountTotal==null){
            map.put("value", 0.0);
        }else{

            map.put("value", orderAmountTotal);
        }
        list.add(map);
        map=null;
        return JSON.toJSONString(list);

    }

    //    http://publisher:8070/realtime-hour?id=dau&date=2019-02-01
    @GetMapping("realtime-hour")
    public String getDauHourTotal(@RequestParam String id, @RequestParam String date) throws ParseException {
        System.out.println(id + ",date=" + date);
        //分时统计	{"yesterday":{"11":383,"12":123,"17":88,"19":200 },
        //"today":{"12":38,"13":1233,"17":123,"19":688 }},得到昨天和今天的数据
        Map<String, Object> hash = new HashMap<>();
        if ("dau".equals(id)) {
            Map<String, Long> todayTotal = dauService.getDauHourTotal(date);

            String yesterday = addDate(date, -1);
            Map<String, Long> yesterdayTotal = dauService.getDauHourTotal(yesterday);
            hash.put("today", todayTotal);
            hash.put("yesterday", yesterdayTotal);
            return JSON.toJSONString(hash);

        }else if("order_amount".equals(id)){

            Map<String, Double> orderAmountHourTotalMap = dauService.getOrderAmountHourTotal(date);
            Map<String, Double> yesterDayOrderAmountHourTotalMap = dauService.getOrderAmountHourTotal(addDate(date, -1));
            hash.put("today", orderAmountHourTotalMap);
            hash.put("yesterday", yesterDayOrderAmountHourTotalMap);
            return JSON.toJSONString(hash);
        }
        else {
            hash.put("today", "no define");
            return JSON.toJSONString(hash);
        }


    }
    @GetMapping("sale_detail")
    //  http://localhost:8070/sale_detail?date=2020-08-15&&startpage=1&size=5&keyword=手机小米
    //期望得到的结果是 年龄阶段 20岁以下，占比20%，20-30岁 占比，40以上占比
    //现在得到的结果，存放的是 每个年龄的个数,性别是 M 30 F 20 =》男 60% 女 30%
    //最终结果可以用map封装，一个大map,一个小map,
    // 也可以用一个对象来封装两个饼图，每个饼图需要的一条数据放到一个对象中，都是一个名词，一个比例
    public String getSaleDetail(@RequestParam("date") String date,@RequestParam("startpage") int pageNum,@RequestParam("size") int pageSize,@RequestParam("keyword") String skuName ){


        Map resultMap = dauService.getSaleDetail(date, skuName, pageSize, pageNum);
        List<Map> saleList = (List<Map>) resultMap.get("saleDetail");
        Map genderMap = (Map)resultMap.get("genderMap");
        Long totalCount = (Long)resultMap.get("total");

        ArrayList<Option> optionList = new ArrayList<>();
        genderMap.forEach((key, value) -> {
            String genderName = (String) key;
            Long count=(Long) value;
//            double v = count.doubleValue();
            double genderRate = Math.round(count * 1000D / totalCount )/ 10D;
            if("M".equals(key)){
                optionList.add(new Option("男", genderRate));

            }else{
                optionList.add(new Option("女", genderRate));
            }

        });
        //性别占比
        OptionGroup genderGroup = new OptionGroup("性别占比", optionList);
        //饼图	男女比例占比	男，女
        //	年龄比例占比	20岁以下，20-30岁，30岁以上
        Map ageMap = (Map)resultMap.get("ageMap");
        Long age_20Count=0L;
        Long age20_30Count = 0L;
        Long age30Count = 0L;
        for (Object entry : ageMap.entrySet()) {
            Map.Entry<String,Long> e = (Map.Entry) entry;
            String agekey = e.getKey();
            Long value = e.getValue();
            int age = Integer.parseInt(agekey);
            if (age<20){
                age_20Count+=1L;

            }else if(age<30){
                age20_30Count += 1L;
            }else{
                age30Count += 1;

            }

        }
        optionList.add(new Option("20岁以下", Math.round(age_20Count * 1000D / totalCount) / 10D));

        optionList.add(new Option("20-30岁", Math.round(age20_30Count * 1000D / totalCount) / 10D));
        optionList.add(new Option("30岁以上", Math.round(age30Count * 1000D / totalCount) / 10D));

        SaleInfo saleInfo = new SaleInfo(totalCount, optionList, saleList);

        return  JSON.toJSONString(saleInfo);

    }


    //根据给定日期获取其他日期
    private String addDate(String date, int days) {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date today = sdf.parse(date);
            Date otherDay = DateUtils.addDays(today, days);
            return sdf.format(otherDay);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }


    }
}
