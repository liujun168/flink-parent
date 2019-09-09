package com.hztq.sc.flink.demo.bk;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * @description: MysqlSinkTest
 * @author: liujun 249489478@qq.com
 * @create: 2019-09-09 14:16
 */
public class MysqlSinkTest {

    //消费消息配置
//    public static void consumer(){
//        Properties p = new Properties();
//        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.89:9092");
//        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        p.put(ConsumerConfig.GROUP_ID_CONFIG, "duanjt_test1");
//
//        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(p);
//        kafkaConsumer.subscribe(Collections.singletonList("duanjt_test"));// 订阅消息
//
//        ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
//        for (ConsumerRecord<String, String> record : records) {
//            System.out.println(String.format("topic:%s,offset:%d,消息:%s", record.topic(), record.offset(), record.value()));
//            int a = 3/0;
//        }
//    }

//    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Properties properties = new Properties();
////        properties.setProperty("bootstrap.servers", "localhost:9092");
//        properties.setProperty("bootstrap.servers", "192.168.1.89:9092");
//        FlinkKafkaConsumer09<String> myConsumer = new FlinkKafkaConsumer09<String>("duanjt_test", new SimpleStringSchema(), properties);
//
//        DataStream<String> stream = env.addSource(myConsumer);
//        DataStream<Tuple2<String, Integer>> counts = stream.flatMap(new LineSplitter());
//
//        //实例化Flink和Redis关联类FlinkJedisPoolConfig，设置Redis端口
////        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
////        //实例化RedisSink，并通过flink的addSink的方式将flink计算的结果插入到redis
////        counts.addSink(new RedisSink<Tuple2<String, Integer>>(conf,new RedisExampleMapper()));
//        try {
//            env.execute("data from kafka");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            System.out.println("value为:"+value);
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                System.out.println("值为:"+token);
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }

}
