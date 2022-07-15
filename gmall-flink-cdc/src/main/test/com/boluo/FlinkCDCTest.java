package com.boluo;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.atguigu.config.Config;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class FlinkCDCTest {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.load(FlinkCDCTest.class.getResourceAsStream("/application.properties"));

        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 通过 Flink CDC 构建SourceFunction 并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname(props.getProperty(Config.MYSQL_CDC1_HOSTNAME))
                .port(Integer.parseInt(props.getProperty(Config.MYSQL_CDC1_PORT)))
                .username(props.getProperty(Config.MYSQL_CDC1_USERNAME))
                .password(props.getProperty(Config.MYSQL_CDC1_PASSWORD))
                .serverTimeZone("Asia/Shanghai")
                .databaseList("db_mp")
                .tableList("db_mp.h_pay")
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        streamSource.print();
        env.execute("Flink CDC");

        // 写入MySQL


    }
}
