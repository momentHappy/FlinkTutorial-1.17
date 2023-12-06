package com.atguigu.split;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO 分流： 奇数、偶数拆分成不同流
 *
 * @author cjp
 * @version 1.0
 */
public class SplitByFilterDemo {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 9000);

        /**
         * TODO 使用filter来实现分流效果
         * 缺点： 同一个数据，要被处理两遍（调用两次filter）
          */

        SingleOutputStreamOperator<String> even = socketDS.filter(value -> Integer.parseInt(value) % 2 == 0);
        SingleOutputStreamOperator<String> odd = socketDS.filter(value -> Integer.parseInt(value) % 2 == 1);


        even.print("偶数流");
        odd.print("奇数流");


        env.execute();
    }
}

/**



 */