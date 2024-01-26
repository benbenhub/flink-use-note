package com.flinkuse.cases.operatorlogic;

import com.flinkuse.core.base.StreamApp;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author learn
 * @date 2024/1/22 15:27
 */
public class StateDistinctLineNumberGroup extends StreamApp {

    private final Integer skuNumber;

    public StateDistinctLineNumberGroup(String[] args, String jobName) {
        super(args, jobName);
        skuNumber = getScpsParams().getInt("bosch_querysku_nmber", 5);
    }

    public static void main(String[] args) throws Exception {
        new StateDistinctLineNumberGroup(args, "StateDistinctLineNumber").start();
    }
    @Override
    public void run(StreamExecutionEnvironment streamEnv) {
        try {
            // reduce算子不同模式有不同结果
            streamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);

            prodGroupLogic(streamEnv.fromElements("3397016430","3397016432","0092S47328","222","3333","44444")).print();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 利用状态去重设置行号,按行号聚合
     * @param inputStream
     * @return
     */
    private DataStream<List<String>> prodGroupLogic(DataStream<String> inputStream) {

        return inputStream.keyBy(value -> value
        ).process(new KeyedProcessFunction<String, String, Tuple2<String, Integer>>() {
            /*
             状态编程
             去重
             */
            private ValueState<Boolean> seen;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化状态描述符
                ValueStateDescriptor<Boolean> stateDescriptor = new ValueStateDescriptor<>("seen", Boolean.class);
                seen = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 检查元素是否已经出现过
                if (seen.value() == null) {
                    // 如果是第一次出现，输出并标记为已出现
                    out.collect(Tuple2.of(value, 1));
                    seen.update(true);
                }
            }
        }).keyBy(k -> k.f1
        ).process(new KeyedProcessFunction<Integer, Tuple2<String, Integer>, Tuple2<List<String>, Long>>() {
            /*
             状态编程
             给每个元素设置行号
             */
            private ValueState<Long> counterState;
            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化状态描述符
                ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("counterState", Long.class);
                counterState = getRuntimeContext().getState(stateDescriptor);
            }
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<List<String>, Long>> out) throws Exception {
                Long currentCount = counterState.value();
                if (currentCount == null) {
                    currentCount = 0L;
                }
                // 更新计数器
                counterState.update(currentCount + 1);
                List<String> t1 = new ArrayList<>();
                t1.add(value.f0);
                Tuple2<List<String>, Long> t = Tuple2.of(t1, currentCount / skuNumber);
                out.collect(t);
            }
        }).keyBy(f -> f.f1
        ).reduce(new RichReduceFunction<Tuple2<List<String>, Long>>() {
            @Override
            public Tuple2<List<String>, Long> reduce(Tuple2<List<String>, Long> v1, Tuple2<List<String>, Long> v2) throws Exception {
                v1.f0.addAll(v2.f0);
                return v1;
            }
        }).map(new MapFunction<Tuple2<List<String>, Long>, List<String>>() {
            @Override
            public List<String> map(Tuple2<List<String>, Long> value) throws Exception {
                return value.f0;
            }
        });
    }
}
