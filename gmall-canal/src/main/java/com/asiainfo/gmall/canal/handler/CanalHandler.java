package com.asiainfo.gmall.canal.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.asiainfo.gmall.canal.utils.MyKafkaSender;
import com.asiainfo.gmall.common.constants.GmallConstants;

import java.util.List;

//用于处理canal解析数据
public class CanalHandler {
    private String tableName;
    private CanalEntry.EventType eventType;
    private List<CanalEntry.RowData> rowDataList;
    public CanalHandler(CanalEntry.EventType eventType,String tableName,List<CanalEntry.RowData> rowDataList){
        this.eventType=eventType;
        this.tableName=tableName;
        this.rowDataList=rowDataList;

    }
    //处理业务逻辑
    public void handle() throws Exception {
        //todo 订单表分析 ：下单操作，涉及到订单状态，是会不断更新的，下单 已发货，待收货，已收货，待评价等，按照之前的离线数仓，
        // 订单表的状态的更新不是改之前的status,而是新增一条，把之前那一条数据的operate 时间修改为当前时间，则该笔订单进入新的状态，所以我们
        // 考虑订单新增情况就行了，同理 订单明细也是如此
        //  用户表就不一样了，用户表涉及到修改，需要把新增和修改都要记录下来
        System.out.println("3 handle 一次 "+tableName +" event_type="+eventType);
        if ("order_info".equals(tableName) && eventType== CanalEntry.EventType.INSERT) {
            System.out.println("4 order info下单操作一次发送kafka数据 "+tableName);
            rowDataList2Kafka(GmallConstants.KAFKA_TOPIC_ORDER_INFO);
        }else if("order_detail".equals(tableName) && eventType==CanalEntry.EventType.INSERT){
            rowDataList2Kafka(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
        }else if("user_info".equals(tableName)&&(eventType==CanalEntry.EventType.INSERT ||eventType==CanalEntry.EventType.UPDATE)){
            System.out.println("4 user_info operation="+eventType);
            rowDataList2Kafka(GmallConstants.KAFKA_TOPIC_USER_INFO);

        }
        //用户新增或者更新操作

    }
    //专门遍历数据，调用kafka发送数据
    private void rowDataList2Kafka(String topic) throws Exception {
        for (CanalEntry.RowData rowData : rowDataList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }
            Thread.sleep(200);
            MyKafkaSender.sendMessage(topic, JSON.toJSONString(jsonObject));
        }


    }

}
