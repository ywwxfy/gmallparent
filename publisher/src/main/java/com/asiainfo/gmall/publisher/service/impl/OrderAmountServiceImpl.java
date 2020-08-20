package com.asiainfo.gmall.publisher.service.impl;

import com.asiainfo.gmall.publisher.mapper.OrderMapper;
import com.asiainfo.gmall.publisher.service.OrderAmountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class OrderAmountServiceImpl implements OrderAmountService {
    @Autowired
    OrderMapper orderMapper;

    //{"yesterday":{"11":383,"12":123,"17":88,"19":200 },
    //"today":{"12":38,"13":1233,"17":123,"19":688 }}

    @Override
    public Double getOrderAmountTotal(String dayString) {
        return orderMapper.getOrderAmountTotal(dayString);
    }

    @Override
    public Map<String, Double> getOrderAmountHourTotal(String dayString) {
        Map<String, Double> hashmap = new HashMap<>();
        List<Map> orderAmountHourTotalMap = orderMapper.getOrderAmountHourTotal(dayString);
        for (Map map : orderAmountHourTotalMap) {
            //利用entry set得到map中 key 和value的值
//            Set<Map.Entry<String,Double>> set = map.entrySet();
//            for (Map.Entry<String, Double> entry : set) {
//                String key = entry.getKey();
//                Double value = entry.getValue();
//            }
                   //直接对map进行遍历
            System.out.println(map.toString());
//            map.iter
            String newKey = (String)map.get("CH");
            Double newValue = (Double) map.get("TM");
//            map.forEach((key, value) -> {
                //
//                System.out.println(key+" ="+value);
//                hashmap.put((String) key, (Double) value);

//            });
            hashmap.put(newKey, newValue);
        }
//        System.out.println(hashmap.toString());
        return hashmap;
    }
}
