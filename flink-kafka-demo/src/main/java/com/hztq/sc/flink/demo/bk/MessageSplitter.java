package com.hztq.sc.flink.demo.bk;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @description: MessageSplitter 将获取到的每条Kafka消息根据“，”分割取出其中的主机名和内存数信息
 * @author: liujun 249489478@qq.com
 * @create: 2019-09-09 15:10
 */
public class MessageSplitter implements FlatMapFunction<String, Tuple2<String, Long>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
        if (value != null && value.contains(",")) {
            String[] parts = value.split(",");
            out.collect(new Tuple2<>(parts[1], Long.parseLong(parts[2])));
        }
    }
}
