package com.atguigu.utils;

import com.atguigu.app.ods.FlinkCDC;
import com.atguigu.config.Config;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.IOException;
import java.util.Properties;

/**
 * @Author dingc
 * @Date 2022-07-15 1:03
 * @Description
 */
public class MyKafkaUtil {

    private static final Properties props = new Properties();

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) throws IOException {
        props.load(MyKafkaUtil.class.getResourceAsStream("/application.properties"));
        String hostname = props.getProperty(Config.KAFKA_SINK1_HOSTNAME);
        String port = props.getProperty(Config.KAFKA_SINK1_PORT);
        return new FlinkKafkaProducer<String>(
                hostname + ":" + port,
                topic,
                new SimpleStringSchema()
        );
    }

}
