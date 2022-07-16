package com.atguigu.utils;

import com.atguigu.config.Config;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.util.Properties;

/**
 * @Author dingc
 * @Date 2022-07-15 1:03
 * @Description
 */
public class MyKafkaUtil {

    private static final Properties props = new Properties();
    private static final String brokers;

    static {
        try {
            props.load(MyKafkaUtil.class.getResourceAsStream("/application.properties"));
            String hostname = props.getProperty(Config.KAFKA_SINK1_HOSTNAME);
            String port = props.getProperty(Config.KAFKA_SINK1_PORT);
            brokers = hostname + ":" + port;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) throws IOException {
        return new FlinkKafkaProducer<String>(
                brokers,
                topic,
                new SimpleStringSchema()
        );
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                properties
        );
    }

}
