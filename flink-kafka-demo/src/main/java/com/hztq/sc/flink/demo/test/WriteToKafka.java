package com.hztq.sc.flink.demo.test;

import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * TODO 此类有问题
 *
 * @description: 向kafka中写数据
 * @author: liujun 249489478@qq.com
 * @create: 2019-09-09 16:59
 */
public class WriteToKafka {
    //定义主题
    public static String topic = "test";

    public static void producerMsg() {
        //kafka配置
        Properties p = new Properties();
        //kafka地址，多个地址用逗号分割
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(p);

        try {
            for (int i = 0; i < 10; i++) {
                String msg = "Hello," + i;
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);
                //kafkaProducer.send(record)可以通过返回的Future来判断是否已经发送到kafka，增强消息的可靠性。
                // 同时也可以使用send的第二个参数来回调，通过回调判断是否发送成功。
                kafkaProducer.send(record);
                System.out.println("发送消息:" + msg);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            kafkaProducer.close();
        }
    }


    public static void main(String[] args) throws Exception {
        producerMsg();
    }

    /**
     * TODO 存在问题
     * 产生消息
     */
    public static void createMsg() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "192.168.1.89:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.89:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        DataStream<String> stream = env.addSource(new SimpleStringGenerator());
        stream.addSink(new FlinkKafkaProducer010("test", new SimpleStringSchema(), properties));  //配置topic
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class SimpleStringGenerator implements SourceFunction<String> {
        long i = 0;
        boolean swith = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            for (int k = 0; k < 5; k++) {
                ctx.collect("flink:" + k++);
                //Thread.sleep(5);  //家里这个后会有问题
            }
        }

        @Override
        public void cancel() {
            swith = false;
        }
    }
}
