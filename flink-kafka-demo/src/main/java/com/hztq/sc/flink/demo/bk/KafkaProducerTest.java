package com.hztq.sc.flink.demo.bk;


//import org.apache.kafka.clients.producer.*;

/**
 * @description: KafkaProducerTest 发送Kafka消息
 * @author: liujun 249489478@qq.com
 * @create: 2019-09-09 15:10
 */
public class KafkaProducerTest {

//    public static void main(String[] args) throws Exception {
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "10.47.85.158:9092");
//        props.put("acks", "all");
//        props.put("retries", 0);
//        props.put("batch.size", 16384);
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//
//        Producer<String, String> producer = new KafkaProducer<>(props);
//        int totalMessageCount = 100;
//        for (int i = 0; i < totalMessageCount; i++) {
//            String value = String.format("%d,%s,%d", System.currentTimeMillis(), "machine-1", currentMemSize());
//            producer.send(new ProducerRecord<>("test-0921", value), new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata metadata, Exception exception) {
//                    if (exception != null) {
//                        System.out.println("Failed to send message with exception " + exception);
//                    }
//                }
//            });
//            Thread.sleep(100L);
//        }
//        producer.close();
//    }

    private static long currentMemSize() {
        return MemoryUsageExtrator.currentFreeMemorySizeInBytes();
    }
}
