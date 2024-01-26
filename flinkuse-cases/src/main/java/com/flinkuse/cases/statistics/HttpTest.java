package com.flinkuse.cases.statistics;

import com.flinkuse.core.base.StreamApp;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author learn
 * @date 2023/4/11 17:50
 */
public class HttpTest extends StreamApp {
    public HttpTest(String[] args) {
        super(args);
    }

    @Override
    public void run(StreamExecutionEnvironment streamEnv) {
        Properties properties = new Properties();
        properties.setProperty("gid.connector.http.sink.header.X-Content-Type-Options", "nosniff");

        DataStream<String> ds = streamEnv.fromCollection(Arrays.asList("234234","3345"));

        sink().HttpSink(ds,"","POST",properties);
    }
}
