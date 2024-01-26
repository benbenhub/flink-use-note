package com.flinkuse.cases.operatorlogic;

import com.flinkuse.core.base.StreamApp;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author learn
 * @date 2024/1/26 17:03
 */
public class ReduceDistinct extends StreamApp {

    public ReduceDistinct(String[] args) {
        super(args);
    }

    public static void main(String[] args) throws Exception {
        new ReduceDistinct(args).start();
    }
    @Override
    public void run(StreamExecutionEnvironment streamEnv) {
        streamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);

        streamEnv.fromElements("222","3397016432","0092S47328","222","3333","44444","55"
        ).keyBy(value -> value
        ).reduce(new RichReduceFunction<>() {
            @Override
            public String reduce(String value1, String value2) throws Exception {
                return value2;
            }
        }).print();

    }
}
