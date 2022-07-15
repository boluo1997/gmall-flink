package com.atguigu.app.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @Author dingc
 * @Date 2022-07-15 1:02
 * @Description
 */
public class CustomerDeserialization implements DebeziumDeserializationSchema<String> {

    // SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={file=DESKTOP-GTT5BK0-bin.000002, pos=2170978, row=1, snapshot=true}} ConnectRecord{topic='mysql_binlog_source.gmall-210325-flink.base_trademark', kafkaPartition=null, key=Struct{id=11}, keySchema=Schema{mysql_binlog_source.gmall_210325_flink.base_trademark.Key:STRUCT}, value=Struct{after=Struct{id=11,tm_name=香奈儿,logo_url=/static/default.jpg},source=Struct{version=1.4.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=0,snapshot=true,db=gmall-210325-flink,table=base_trademark,server_id=0,file=DESKTOP-GTT5BK0-bin.000002,pos=2170978,row=0},op=c,ts_ms=1657804181988}, valueSchema=Schema{mysql_binlog_source.gmall_210325_flink.base_trademark.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
    // SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={file=DESKTOP-GTT5BK0-bin.000002, pos=2170978}} ConnectRecord{topic='mysql_binlog_source.gmall-210325-flink.base_trademark', kafkaPartition=null, key=Struct{id=12}, keySchema=Schema{mysql_binlog_source.gmall_210325_flink.base_trademark.Key:STRUCT}, value=Struct{after=Struct{id=12,tm_name=菠萝,logo_url=/boluo},source=Struct{version=1.4.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=0,snapshot=last,db=gmall-210325-flink,table=base_trademark,server_id=0,file=DESKTOP-GTT5BK0-bin.000002,pos=2170978,row=0},op=c,ts_ms=1657804181988}, valueSchema=Schema{mysql_binlog_source.gmall_210325_flink.base_trademark.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}

    // {"database":"gmall-210325-flink","before":{"tm_name":"Redmi","id":1},"after":{},"type":"insert","table":"base_trademark"}
    // {"database":"gmall-210325-flink","before":{"tm_name":"苹果","logo_url":"/static/default.jpg","id":2},"after":{},"type":"insert","table":"base_trademark"}
    // {"database":"gmall-210325-flink","before":{"tm_name":"华为","logo_url":"/static/default.jpg","id":3},"after":{},"type":"insert","table":"base_trademark"}

    @Override
    @SuppressWarnings("all")
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        // 1.创建json对象用于存储最终数据
        JSONObject result = new JSONObject();

        // 2.获取数据库名和表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String table = fields[2];

        // 3.获取"before"数据
        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            Schema beforeSchema = before.schema();
            List<Field> beforeFields = beforeSchema.fields();
            for (Field field : beforeFields) {
                Object beforeValue = before.get(field);
                beforeJson.put(field.name(), beforeValue);
            }
        }

        // 4.获取"after"数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            Schema afterSchema = after.schema();
            List<Field> afterFields = afterSchema.fields();
            for (Field field : afterFields) {
                Object afterValue = after.get(field);
                beforeJson.put(field.name(), afterValue);
            }
        }

        // 5.获取操作类型 CREATE UPDATE DELETE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        // 6.将字段写入json对象
        result.put("database", database);
        result.put("table", table);
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("type", type);

        // 7.输出数据
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

}
