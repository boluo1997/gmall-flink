package com.boluo;

import config.Config;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Properties;

public class FlinkSqlCDCTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties props = new Properties();
        props.load(FlinkSqlCDCTest.class.getResourceAsStream("/application.properties"));
        Configuration configuration = new Configuration();
        props.stringPropertyNames().forEach(key -> {
            String value = props.getProperty(key);
            configuration.setString(key.trim(), StringUtils.isBlank(value) ? "" : value.trim());
        });
        env.getConfig().setGlobalJobParameters(configuration);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // TableConfig tableConfig = tableEnv.getConfig();
        // tableConfig.setIdleStateRetention(Duration.ofMinutes(1L));

        // ------------------------------- mp_conf_agent ------------------------------------------------
        tableEnv.executeSql("CREATE TABLE `cdc_source` (\n" +
                "  `id` BIGINT PRIMARY KEY NOT ENFORCED,\n" +
                "  `conf_id` INT,\n" +
                "  `app_id` INT,\n" +
                "  `agent_id` INT,\n" +
                "  `advertiser_conf_id` INT,\n" +
                "  `create_time` BIGINT,\n" +
                "  `update_time` BIGINT\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = '" + props.getProperty(Config.MYSQL_CDC1_HOSTNAME) + "',\n" +
                " 'port' = '" + props.getProperty(Config.MYSQL_CDC1_PORT) + "',\n" +
                " 'username' = '" + props.getProperty(Config.MYSQL_CDC1_USERNAME) + "',\n" +
                " 'password' = '" + props.getProperty(Config.MYSQL_CDC1_PASSWORD) + "',\n" +
                " 'database-name' = '" + props.getProperty(Config.MYSQL_CDC1_DATABASE) + "',\n" +
                " 'table-name' = 'mp_conf_agent',\n" +
                " 'server-time-zone' = 'Asia/Shanghai'" +
                ")");

        // Table table = tableEnv.sqlQuery("select * from cdc_source");
        // DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        // retractStream.print();
        // env.execute("Flink CDC");
        // TableResult res = tableEnv.executeSql("select * from cdc_source");
        // res.print();

        tableEnv.executeSql("CREATE TABLE `cdc_sink` (\n" +
                "  `id` BIGINT PRIMARY KEY NOT ENFORCED,\n" +
                "  `conf_id` INT,\n" +
                "  `app_id` INT,\n" +
                "  `agent_id` INT,\n" +
                "  `advertiser_conf_id` INT,\n" +
                "  `create_time` BIGINT,\n" +
                "  `update_time` BIGINT\n" +
                ") WITH (\n" +
                " 'connector' = 'jdbc',\n" +
                " 'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                " 'url' = '" + props.getProperty(Config.MYSQL_SINK1_URL) + "',\n" +
                " 'username' = '" + props.getProperty(Config.MYSQL_SINK1_USERNAME) + "',\n" +
                " 'password' = '" + props.getProperty(Config.MYSQL_SINK1_PASSWORD) + "',\n" +
                " 'table-name' = 'boluo_test'\n" +
                ")");

        tableEnv.executeSql("INSERT INTO cdc_sink SELECT * FROM cdc_source");

    }

}
