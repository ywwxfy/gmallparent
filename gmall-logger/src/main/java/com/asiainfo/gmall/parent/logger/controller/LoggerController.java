package com.asiainfo.gmall.parent.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.asiainfo.gmall.common.constants.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

//@Controller 方法要加上 responsebody,才能不把字符串解析为一个新的跳转
@RestController
@Slf4j
public class LoggerController {
    @Autowired
   KafkaTemplate<String,String> kafkaTemplate;

//    @GetMapping("/log")
//    @RequestMapping("log")
    @PostMapping("/log")
//    @ResponseBody
    public String log(@RequestParam String logString){
//        System.out.println(logString);
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());
        //日志落盘
        String jsonString = jsonObject.toJSONString();
        log.info(jsonString);
        //日志分了类型，启动日志和事件日志
        if ("startup".equals(jsonObject.getString("type"))) {

            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP, jsonString);

        } else {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT, jsonString);
        }
        return "success";


    }

}
