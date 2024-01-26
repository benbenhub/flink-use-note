package com.flinkuse.core.connector.redis;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import redis.clients.jedis.Jedis;

import java.io.IOException;

/**
 * @author learn
 * @date 2023/2/9 22:13
 */
public abstract class RedisOutputFormat<T> extends RichOutputFormat<T> {

    private transient RedisSyncClient redisSyncClient;
    protected Jedis jedis;

    public RedisOutputFormat() {
    }

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int i, int i1) throws IOException {
        Configuration parameters = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        redisSyncClient = new RedisSyncClient(parameters);
        jedis = redisSyncClient.getJedis();
    }

    @Override
    public void writeRecord(T r) {
        try {
            writeSingleRecordWithRetry(r);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public abstract void writeSingleRecordWithRetry(T r) throws Exception;

    @Override
    public void close() throws IOException {
        redisSyncClient.close(jedis);
    }
}
