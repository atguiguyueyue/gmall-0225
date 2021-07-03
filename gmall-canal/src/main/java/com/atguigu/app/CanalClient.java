package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;

public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1.获取canal连接器

        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        //目的为了一直监控mysql数据库，所以要写到死循环，否则当main方法执行完毕，连接就断掉了
        while (true) {
            //2.通过连接器获取canal连接
            canalConnector.connect();

            //3.选择要订阅的数据库
            canalConnector.subscribe("gmall.*");

            //4.获取多个sql执行的结果
            Message message = canalConnector.get(100);

            //5.获取每个sql执行结果的entry
            List<CanalEntry.Entry> entries = message.getEntries();

            //6.判断是否有数据生成
            if (entries.size()>0){
                //7.获取每个entry
                for (CanalEntry.Entry entry : entries) {
                    //TODO 8.获取表名
                    String tableName = entry.getHeader().getTableName();

                    //9.获取entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();

                    //10.判断entry类型，如果是rowData则处理（因为rowdata里面才是数据）
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)){
                        //11.获取序列化数据
                        ByteString storeValue = entry.getStoreValue();

                        //12.反序列化数据
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        //TODO 13.获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //TODO 14.获取每一行数据集（一个sql所对应的真正数据）
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        hadle(tableName, eventType, rowDatasList);
                    }

                }
            }


        }

    }

    /**
     * 解析具体的数据
     * @param tableName
     * @param eventType
     * @param rowDatasList
     */
    private static void hadle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        if ("order_info".equals(tableName)&&CanalEntry.EventType.INSERT.equals(eventType)){
            //1.获取每一条数据
            for (CanalEntry.RowData rowData : rowDatasList) {
                //2.获取更新之后的数据
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                //3.获取每一条数据
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : afterColumnsList) {
                    //4.获取字段名，和具体的值并封装到JSON中
                    jsonObject.put(column.getName(), column.getValue());
                }
                System.out.println(jsonObject.toString());
                //5.将数据发送至kafka的topic中
                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER, jsonObject.toString());
            }
        }
    }
}
