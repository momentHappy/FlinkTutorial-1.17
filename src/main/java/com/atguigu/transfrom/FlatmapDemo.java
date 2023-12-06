package com.atguigu.transfrom;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * TODO 如果输入的数据是sensor_1，只打印vc；如果输入的数据是sensor_2，既打印ts又打印vc
 *
 * @author cjp
 * @version 1.0
 */
public class FlatmapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );


        /**
         * TODO flatmap： 一进多出（包含0出）
         *      对于s1的数据，一进一出
         *      对于s2的数据，一进2出
         *      对于s3的数据，一进0出（类似于过滤的效果）
         *
         *    map怎么控制一进一出：
         *      =》 使用 return
         *
         *    flatmap怎么控制的一进多出
         *      =》 通过 Collector来输出， 调用几次就输出几条
         *
         *
         */
        SingleOutputStreamOperator<String> flatmap = sensorDS.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
                if ("s1".equals(value.getId())) {
                    // 如果是 s1，输出 vc
                    out.collect(value.getVc().toString());
                } else if ("s2".equals(value.getId())) {
                    // 如果是 s2，分别输出ts和vc
                    out.collect(value.getTs().toString());
                    out.collect(value.getVc().toString());
                }
            }
        });

        flatmap.print();


        env.execute();
    }


}
