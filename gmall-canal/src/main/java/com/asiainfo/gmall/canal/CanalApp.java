package com.asiainfo.gmall.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.asiainfo.gmall.canal.handler.CanalHandler;
import com.asiainfo.gmall.common.constants.GmallConstants;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

//通用监视类，监视mysql数据库发生变化
//把这些变化的数据发送到kafka中，最后保存到hbase数据库中
public class CanalApp {

    public static void main(String[] args) throws InvalidProtocolBufferException {
        //TODO 1 用监视类，监视mysql数据库发生变化
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("s102", 11111),
                "example", "", "");
        //TODO 2 不停的抓取数据
//        int[] a={0,1,2,3};
//        String[] b=new String[]{"a","b","c"};
            while (true) {
            canalConnector.connect();
            canalConnector.subscribe(GmallConstants.CANAL_WATCH_DB);
            //每次抓取10个sql变化产生的结果集，可能是很多条记录
            Message message = canalConnector.get(100);
            List<CanalEntry.Entry> entries = message.getEntries();
            if (entries.size() == 0) {
//                System.out.println("当前时间段没有数据变化，休息5s");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("1 开始处理变化数据 "+entries.size());
                CanalHandlEntries(entries);

            }
        }


    }

    private static void CanalHandlEntries(List<CanalEntry.Entry> entries) throws InvalidProtocolBufferException {
        //循环处理数据，是否返回对象还是直接调用，需要分析会产生什么样的结果
        for (CanalEntry.Entry entry : entries) {
//            String tableName = entry.getHeader().getTableName();
            //序列化以后的对象
            ByteString storeValue = entry.getStoreValue();
            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                String tableName = entry.getHeader().getTableName();
                System.out.println("2 处理一次entry rowdata 操作 tablename="+tableName);
                CanalEntry.RowChange rowChange = null;
                //反序列化得到的行变化集,如果是数据方面的变化才反序列化，其他不管
                rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                CanalHandler handler = new CanalHandler(rowChange.getEventType(), tableName, rowDatasList);
                try {
                    handler.handle();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                //如果是插入的操作，之前记录就为null,只取操作之后的
//                            if ("order_info".equals(tableName) && rowChange.getEventType()== CanalEntry.EventType.INSERT) {
//
//                                for (CanalEntry.RowData rowData : rowDatasList) {
//                                    List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
//                                    for (CanalEntry.Column column : afterColumnsList) {
//                                        String name = column.getName();
//                                        String value = column.getValue();
//                                        System.out.println(name+"="+value);
//
//                                    }
//                                }
//                            }


            }
        }
    }
}