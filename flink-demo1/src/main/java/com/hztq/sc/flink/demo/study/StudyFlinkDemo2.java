package com.hztq.sc.flink.demo.study;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @description: 学习flinkDemo2
 * @author: liujun 249489478@qq.com
 * @create: 2019-09-06 15:07
 */
public class StudyFlinkDemo2 {
    //初始化flink运行环境
    static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    /**
     * 创建测试数据
     */
    private static DataSet<String> creatTestData() {
        System.out.println("开始执行任务.....................");
        //DataSet 是一个抽象类
        DataSet<String> data = env.fromElements("To be, or not to be,--that is the question:--",
                "Whether 'tis nobler in the mind to suffer",
                "The slings and arrows of outrageous fortune",
                "Or to take arms against a sea of troubles,");
        return data;
    }

    public static void main(String[] args) {
        DataSet<String> text = creatTestData();
        // 执行WordCount
        DataSet<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new LineSplitter())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1);

        // 打印结果
        try {
            counts.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

//实现了FlatMapFunction接口
class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
    // 统一大小写，并切割
        String[] tokens = value.toLowerCase().split("\\W+");
        // 构造成(word,1)的格式
        for (String token : tokens) {
            if (token.length() > 0) {
                collector.collect(new Tuple2<String, Integer>(token, 1));
            }
        }
    }
}
