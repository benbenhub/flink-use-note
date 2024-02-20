package com.flinkuse.core.connector.mongodb;

import com.flinkuse.core.constance.ConfigKeys;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.*;

/**
 * @author learn
 * @date 2023/8/9 13:54
 */
@Slf4j
public abstract class MongodbAsyncFormat<IN,OUT> extends RichAsyncFunction<IN,OUT> {

    private MongoClient mongoClient;
    private MongoCollection mongoCollection;
    private int nThreads;
    private ExecutorService executorService;
    private final String database;
    private final String collection;

    public MongodbAsyncFormat(String database, String collection) {
        this.database = database;
        this.collection = collection;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        parameters = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        nThreads = parameters.getInteger(ConfigKeys.mongodb_async_threads_num);
        mongoClient = MongodbClientBase.getClient(parameters, database);
        // 创建集合
        mongoCollection = mongoClient.getDatabase(database).getCollection(collection);
        executorService = Executors.newFixedThreadPool(nThreads);
    }
    @Override
    public void close() throws SQLException {
        if (mongoClient != null)
            mongoClient.close();
    }

    @Override
    public void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) throws Exception {

        Future<OUT> dbResult = executorService.submit(() -> asyncInvokeInput(input, mongoCollection));

        CompletableFuture.supplyAsync( () -> {
            try {
                return dbResult.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("Mongodb async input data:[{}]", input.toString(), e);
                return null;
            }
        }).thenAccept( (OUT data) -> {
            if(data != null) {
                resultFuture.complete(Collections.singleton(data));
            } else {
                resultFuture.complete(Collections.emptySet());
            }
        });
    }

    public abstract OUT asyncInvokeInput(IN input, MongoCollection mongoCollection) throws Exception;

}
