package com.flinkuse.cases.connector;

import com.alibaba.fastjson.JSON;
import com.flinkuse.core.base.StreamApp;
import com.flinkuse.core.connector.http.HttpClientAsyncFormat;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author learn
 * @date 2024/1/26 15:30
 */
public class AsyncHttp extends StreamApp {
    public AsyncHttp(String[] args) {
        super(args);
    }

    @Override
    public void run(StreamExecutionEnvironment streamEnv) {
        DataStream<String> data = streamEnv.fromElements("3397016430","3397016432","0092S47328","222","3333","44444");

        DataStream<String> skuRes =
                AsyncDataStream.orderedWait(data, new HttpClientAsyncFormat<>() {
                    @Override
                    public HttpRequestBase asyncInvokeHandle(String o) throws Exception {
                        return httpAccess(o);
                    }
                }, 1, TimeUnit.MINUTES, 5);

        skuRes.print();
    }

    private HttpRequestBase httpAccess(String o) {
        HttpPost httpPost = new HttpPost("www.baidu.com");
        httpPost.addHeader("Content-Type", "application/json");
        // httpPost.addHeader("authorization", authorization);

        Map<String, String> d = new HashMap<>();
        d.put("args", o);
        StringEntity entity = new StringEntity(JSON.toJSONString(d), StandardCharsets.UTF_8);
        entity.setContentEncoding(StandardCharsets.UTF_8.toString());
        entity.setContentType("application/json");
        httpPost.setEntity(entity);

        return httpPost;
    }
}
