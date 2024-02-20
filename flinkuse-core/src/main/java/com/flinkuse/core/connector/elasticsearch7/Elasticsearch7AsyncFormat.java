package com.flinkuse.core.connector.elasticsearch7;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

/**
 * @author learn
 * @date 2022/5/5 16:23
 */
public abstract class Elasticsearch7AsyncFormat<IN,OUT> extends RichAsyncFunction<IN, OUT> {

    private transient RestHighLevelClient restHighLevelClient;

    @Override
    public void open(Configuration parameters) throws KeyManagementException, NoSuchAlgorithmException {
        parameters = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        restHighLevelClient = new RestHighLevelClient(Elasticsearch7ClientBase.getRestClientBuilder(parameters));
    }

    @Override
    public void close() throws IOException {
        if (restHighLevelClient != null)
            restHighLevelClient.close();
    }

    @Override
    public void asyncInvoke(IN in, ResultFuture<OUT> resultFuture) throws Exception {
//        restHighLevelClient.searchAsync();
    }
}
