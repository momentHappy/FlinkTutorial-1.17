package com.atguigu.aggreagte;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s1", 21L, 21),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );


        KeyedStream<WaterSensor, String> sensorKS = sensorDS
                .keyBy(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor value) throws Exception {
                        return value.getId();
                    }
                });

        /**
         * TODO reduce:
         * 1、keyby之后调用
         * 2、输入类型 = 输出类型，类型不能变
         * 3、每个key的第一条数据来的时候，不会执行reduce方法，存起来，直接输出
         * 4、reduce方法中的两个参数
         *     value1： 之前的计算结果，存状态
         *     value2： 现在来的数据
         */
        SingleOutputStreamOperator<WaterSensor> reduce = sensorKS.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                System.out.println("value1=" + value1);
                System.out.println("value2=" + value2);
                return new WaterSensor(value1.id, value2.ts, value1.vc + value2.vc);
            }
        });

        reduce.print();


        env.execute();
    }


}
