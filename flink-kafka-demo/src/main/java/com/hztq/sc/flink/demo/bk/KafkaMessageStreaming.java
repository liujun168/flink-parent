package com.hztq.sc.flink.demo.bk;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import java.util.Properties;
/**
 * @description: KafkaMessageStreaming Flink入口类，封装了对于Kafka消息的处理逻辑。本例每1秒统计一次结果并写入到本地件或者打印出来
 * @author: liujun 249489478@qq.com
 * @create: 2019-09-09 15:06
 */

public class KafkaMessageStreaming {

//    public static void main(String[] args) throws Exception {
//
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // 非常关键，一定要设置启动检查点！！
//        //默认情况下，检查点被禁用。要启用检查点，请在StreamExecutionEnvironment上调用enableCheckpointing(n)方法，
//        // 其中n是以毫秒为单位的检查点间隔。每隔5000 ms进行启动一个检查点,则下一个检查点将在上一个检查点完成后5秒钟内启动
//        env.enableCheckpointing(5000);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        Properties props = new Properties();
//        props.setProperty("bootstrap.servers", "192.168.1.89:9092");
//        props.setProperty("group.id", "flink-group");
//        FlinkKafkaConsumer09<String> myConsumer = new FlinkKafkaConsumer09<String>("test0", new SimpleStringSchema(),
//                props);//test0是kafka中开启的topic
//        myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());
//        DataStream<String> keyedStream = env.addSource(myConsumer);//将kafka生产者发来的数据进行处理，本例子我进任何处理
//        keyedStream.print();//直接将从生产者接收到的数据在控制台上进行打印
//        // execute program
//        env.execute("Flink Streaming Java API Skeleton");
//
////        DataStream<String> consumer = env.addSource(new FlinkKafkaConsumer08<>("duanjt_test1", new SimpleStringSchema(), props));
//        //    args[0] = "test-0921";  //传入的是kafka中的topic
////        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("duanjt_test1", new SimpleStringSchema(), props);
////        FlinkKafkaConsumer08<String> consumer = new FlinkKafkaConsumer08<>("duanjt_test1", new SimpleStringSchema(), props);
////        consumer.assignTimestampsAndWatermarks(new MessageWaterEmitter());
////        consumer.print();
//
////        DataStream<Tuple2<String, Long>> keyedStream = env
////                .addSource(consumer)
////                .flatMap(new MessageSplitter())
////                .keyBy(0)
////                .timeWindow(Time.seconds(2))
////                .apply(new WindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow>() {
////                    @Override
////                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple2<String, Long>> out) throws Exception {
////                        long sum = 0L;
////                        int count = 0;
////                        for (Tuple2<String, Long> record: input) {
////                            sum += record.f1;
////                            count++;
////                        }
////                        Tuple2<String, Long> result = input.iterator().next();
////                        result.f1 = sum / count;
////                        out.collect(result);
////                    }
////                });
//
//        //将结果打印出来
////        keyedStream.print();
//        //    将结果保存到文件中
//        //    args[1] = "E:\\FlinkTest\\KafkaFlinkTest";//传入的是结果保存的路径
////        keyedStream.writeAsText("E:\\FlinkTest\\KafkaFlinkTest");
////        env.execute("Kafka-Flink Test");
//    }
}

class CustomWatermarkEmitter implements AssignerWithPunctuatedWatermarks<String> {

    private static final long serialVersionUID = 1L;

    @Override
    public long extractTimestamp(String arg0, long arg1) {
        if (null != arg0 && arg0.contains(",")) {
            String parts[] = arg0.split(",");
            return Long.parseLong(parts[0]);
        }
        return 0;
    }

    @Override
    public Watermark checkAndGetNextWatermark(String arg0, long arg1) {
        if (null != arg0 && arg0.contains(",")) {
            String parts[] = arg0.split(",");
            return new Watermark(Long.parseLong(parts[0]));
        }
        return null;
    }
}