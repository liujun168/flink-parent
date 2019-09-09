package com.hztq.sc.flink.demo.bk;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
/**
 * @description: MessageWaterEmitter 根据Kafka消息确定Flink的水位
 * @author: liujun 249489478@qq.com
 * @create: 2019-09-09 15:09
 */
public class MessageWaterEmitter implements AssignerWithPunctuatedWatermarks<String> {

    @Override
    public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
        if (lastElement != null && lastElement.contains(",")) {
            String[] parts = lastElement.split(",");
            return new Watermark(Long.parseLong(parts[0]));
        }
        return null;
    }

    @Override
    public long extractTimestamp(String element, long previousElementTimestamp) {
        if (element != null && element.contains(",")) {
            String[] parts = element.split(",");
            return Long.parseLong(parts[0]);
        }
        return 0L;
    }
}