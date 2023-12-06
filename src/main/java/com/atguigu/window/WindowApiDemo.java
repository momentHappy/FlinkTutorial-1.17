package com.atguigu.window;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class WindowApiDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("localhost", 9000)
                .map(new WaterSensorMapFunction());


        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(WaterSensor::getId);




        // TODO 1. 指定 窗口分配器： 指定 用 哪一种窗口 ---  时间 or 计数？ 滚动、滑动、会话？
        // 1.1 没有keyby的窗口: 窗口内的 所有数据 进入同一个 子任务，并行度只能为1
//        sensorDS.windowAll()
        // 1.2 有keyby的窗口: 每个key上都定义了一组窗口，各自独立地进行统计计算

        // 基于时间的
//        sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // 滚动窗口，窗口长度10s
//        sensorKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2))) // 滑动窗口，窗口长度10s，滑动步长2s
//        sensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))) // 会话窗口，超时间隔5s

        // 基于计数的
//        sensorKS.countWindow(5)  // 滚动窗口，窗口长度=5个元素
//        sensorKS.countWindow(5,2) // 滑动窗口，窗口长度=5个元素，滑动步长=2个元素
//        sensorKS.window(GlobalWindows.create())  // 全局窗口，计数窗口的底层就是用的这个，需要自定义的时候才会用

        // TODO 2. 指定 窗口函数 ： 窗口内数据的 计算逻辑
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 增量聚合： 来一条数据，计算一条数据，窗口触发的时候输出计算结果
//        sensorWS
//                .reduce()
//        .aggregate(, )

        // 全窗口函数：数据来了不计算，存起来，窗口触发的时候，计算并输出结果
//        sensorWS.process()

        env.execute();
    }
}
