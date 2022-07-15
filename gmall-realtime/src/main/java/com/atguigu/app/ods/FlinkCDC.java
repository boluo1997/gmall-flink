package com.atguigu.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.function.CustomerDeserialization;
import com.atguigu.config.Config;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Author dingc
 * @Date 2022-07-15 1:02
 * @Description
 */
public class FlinkCDC {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.load(FlinkCDC.class.getResourceAsStream("/application.properties"));

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.通过 Flink CDC 构建SourceFunction 并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname(props.getProperty(Config.MYSQL_CDC1_HOSTNAME))
                .port(Integer.parseInt(props.getProperty(Config.MYSQL_CDC1_PORT)))
                .username(props.getProperty(Config.MYSQL_CDC1_USERNAME))
                .password(props.getProperty(Config.MYSQL_CDC1_PASSWORD))
                .databaseList("db_mp")
                .tableList("db_mp.mp_conf_agent")
                .serverTimeZone("Asia/Shanghai")
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        // 3.打印数据, 写入Kafka
        streamSource.print();
        String sinkTopic = "ods_base_db";
        streamSource.addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        // 4.启动任务
        env.execute("FlinkCDCWithCustomerDeserialization");
    }

}
