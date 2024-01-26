package com.flinkuse.core.connector.http;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

public abstract class HttpClientAsyncFormat<IN> extends RichAsyncFunction<IN,String> {

    private CloseableHttpAsyncClient http_client = null;
    protected HttpUrlAccessFunction httpUrlAccessFunction;

    @Override
    public void open(Configuration parameters) throws Exception {

        parameters = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        http_client = new HttpUrlOutputBase(parameters).createCloseableHttpAsyncClient();

        httpUrlAccessFunction = new HttpUrlAccessFunction();
    }

    @Override
    public void close() throws Exception {
        if( http_client != null)
            http_client.close();
    }

    @Override
    public void timeout(IN input, ResultFuture<String> resultFuture) throws Exception {
        resultFuture.completeExceptionally(new RuntimeException(" HttpClient async op timeout "));
    }

    @Override
    public void asyncInvoke(IN in, ResultFuture<String> resultFuture) throws Exception {
        http_client.start();
        http_client.execute(asyncInvokeInputHandle(in), new FutureCallback<>() {
            @Override
            public void completed(HttpResponse httpResponse) {
                try {
                    resultFuture.complete(Collections.singleton(EntityUtils.toString(httpResponse.getEntity(), StandardCharsets.UTF_8)));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void failed(Exception e) {
                e.printStackTrace();
            }

            @Override
            public void cancelled() {
                resultFuture.complete(Collections.emptySet());
            }
        });
    }

    public abstract HttpRequestBase asyncInvokeInputHandle(IN in) throws Exception;


}
