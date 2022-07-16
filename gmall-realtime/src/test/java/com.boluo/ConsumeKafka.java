package com.boluo;

import com.atguigu.app.ods.FlinkCDC;
import com.atguigu.config.Config;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class ConsumeKafka {

    // 消费kafka中的数据
    public static void main(String[] args) throws Exception {

        Properties info = new Properties();
        info.load(FlinkCDC.class.getResourceAsStream("/application.properties"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String hostname = info.getProperty(Config.KAFKA_SINK1_HOSTNAME);
        String port = info.getProperty(Config.KAFKA_SINK1_PORT);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, hostname + ":" + port);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-group");
        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer<String>("ods_base_db", new SimpleStringSchema(), properties));

        //3.将数据打印
        kafkaDS.print();

        //4.执行任务
        env.execute();

    }

}
