package com.flinkuse.core.connector.jdbc;

import com.flinkuse.core.constance.ConfigKeys;
import com.flinkuse.core.enums.JdbcType;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author learn
 * @date 2022/8/3 9:55
 */
@Slf4j
public abstract class JdbcAsyncFormat<IN,OUT> extends RichAsyncFunction<IN,OUT> {

    private JdbcConnectionPool jdbcConnect;
    private JdbcStatement jsf;
    private final JdbcType jdbcType;
    private ExecutorService executorService;

    public JdbcAsyncFormat(JdbcType jdbcType){
        this.jdbcType = jdbcType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        parameters = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        switch (jdbcType){
            case clickhouse:
                initClickhouse(parameters);
                break;
            case doris:
                initDoris(parameters);
                break;
        }

        executorService = jdbcConnect.getExecutorService();
    }
    @Override
    public void close() throws SQLException {
        if(jdbcConnect != null)
            jdbcConnect.close();
    }

    @Override
    public void asyncInvoke(IN element, ResultFuture<OUT> resultFuture) throws Exception {

        Future<OUT> dbResult = executorService.submit(() -> asyncInvokeHandle(element, jsf));

        CompletableFuture.supplyAsync( () -> {
            try {
                return dbResult.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("JDBC async input data:[{}]", element.toString(), e);
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

    public abstract OUT asyncInvokeHandle(IN element, JdbcStatement jdbcQueryFunction) throws Exception;

    private void initClickhouse(Configuration parameters) throws Exception {
        jdbcConnect = JdbcConnectionPool.buildJdbcConnectionPool()
                .setJdbcUrl(String.format("jdbc:clickhouse://%s:%s/%s?%s",
                        parameters.get(ConfigKeys.clickhouse_host)
                        , parameters.get(ConfigKeys.clickhouse_port)
                        , parameters.get(ConfigKeys.clickhouse_database)
                        , "socket_timeout="+parameters.get(ConfigKeys.clickhouse_socket_timeout).toString())
                ).setUserName(parameters.get(ConfigKeys.clickhouse_username))
                .setPassword(parameters.get(ConfigKeys.clickhouse_password))
                .setDriverName(parameters.get(ConfigKeys.clickhouse_driver))
                .setMaxPoolConn(parameters.getInteger(ConfigKeys.clickhouse_maximumConnection))
                .finish();

        jsf = new JdbcStatement(jdbcConnect);
    }
    private void initDoris(Configuration parameters) throws Exception {
        jdbcConnect = JdbcConnectionPool.buildJdbcConnectionPool()
                .setJdbcUrl(String.format("jdbc:mysql://%s:%s/%s?autoReconnect=true&connectTimeout=%s&socketTimeout=%s",
                        parameters.get(ConfigKeys.doris_host)
                        , parameters.get(ConfigKeys.doris_port)
                        , parameters.get(ConfigKeys.doris_database)
                        ,parameters.getInteger(ConfigKeys.connectTimeout)
                        ,parameters.getInteger(ConfigKeys.socketTimeout)
                        )
                ).setUserName(parameters.get(ConfigKeys.doris_username))
                .setPassword(parameters.get(ConfigKeys.doris_password))
                .setDriverName(parameters.get(ConfigKeys.doris_driver))
                .setMaxPoolConn(parameters.getInteger(ConfigKeys.doris_maximumConnection))
                .setInitialSize(parameters.get(ConfigKeys.doris_initialSize))
                .setMinIdle(parameters.getInteger(ConfigKeys.doris_minIdle))
                .setMaxWait(parameters.getInteger(ConfigKeys.doris_maxWait))
                .setTimeBetweenEvictionRunsMillis(parameters.getInteger(ConfigKeys.doris_timeBetweenEvictionRunsMillis))
                .setMinEvictableIdleTimeMillis(parameters.getInteger(ConfigKeys.doris_minEvictableIdleTimeMillis))
                .setMaxEvictableIdleTimeMillis(parameters.getInteger(ConfigKeys.doris_maxEvictableIdleTimeMillis))
                .setPhyTimeoutMillis(parameters.getInteger(ConfigKeys.doris_phyTimeoutMillis))
                .setKeepAlive(parameters.getBoolean(ConfigKeys.doris_keepAlive))
                .setKeepAliveBetweenTimeMillis(parameters.getInteger(ConfigKeys.doris_keepAliveBetweenTimeMillis))
                .setTestWhileIdle(parameters.getBoolean(ConfigKeys.doris_testWhileIdle))
                .setUsePingMethod(parameters.getBoolean(ConfigKeys.doris_usePingMethod))
                .finish();
        jsf = new JdbcStatement(jdbcConnect);
    }
}
