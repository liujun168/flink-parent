package com.hztq.sc.flink.demo.study;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @description: flink测试
 * @author: liujun 249489478@qq.com
 * @create: 2019-09-06 14:27
 */
public class StudyFlinkDemo1 {

    public static void main(String[] args) throws Exception {
        //获取运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从文件中读取-- dubug有输出;run没有输出结果
        DataStreamSource<String> text = env.readTextFile("D:\\source\\idea\\test\\project\\flink\\flink-parent\\flink-demo1\\src\\main\\resources\\demo.txt");

        //计算数据
        DataStream<WordWithCount> windowCount = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
//                String[] splits = value.split("\\s");
                String[] splits = value.split("\n");
                for (String word : splits) {
                    out.collect(new WordWithCount(word, 1L));
                }
            }
        })//打平操作，把每行的单词转为<word,count>类型的数据
                .keyBy("word")//针对相同的word数据进行分组
                .timeWindow(Time.seconds(2), Time.seconds(1))//指定计算数据的窗口大小和滑动窗口大小
                .sum("count");

        //把数据打印到控制台
        windowCount.print().setParallelism(1);//使用一个并行度
        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("streaming word count");

//        windowCount.print();

    }

    /**
     * 主要为了存储单词以及单词出现的次数
     */
    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

}