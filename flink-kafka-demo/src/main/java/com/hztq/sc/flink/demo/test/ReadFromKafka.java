package com.hztq.sc.flink.demo.test;


import java.util.Date;
import java.util.Properties;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
/**
 * @description: 从kafka中读取数据
 * @author: liujun 249489478@qq.com
 * @create: 2019-09-09 16:57
 */
public class ReadFromKafka {
    public static void main(String[] args) throws Exception {
        // 构建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        //这里是由一个kafka
        properties.setProperty("bootstrap.servers", "192.168.1.89:9092");
        properties.setProperty("group.id", "flink_consumer");
        //第一个参数是topic的名称
        DataStream<String> stream=env.addSource(new FlinkKafkaConsumer010("test", new SimpleStringSchema(), properties));
        //直接将从生产者接收到的数据在控制台上进行打印
//        stream.print();
        //直接将从生产者接收到的数据在控制台上进行打印
        stream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
//                return new Date().toString()+":  "+value;
                return "from kafka message"+":  "+value;
            }
        }).print();
        env.execute();


    }

}
