package com.flinkuse.core.connector.redis;

import com.flinkuse.core.constance.ConfigKeys;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RedisSyncClient {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSyncClient.class);

    private JedisPool pool;

    private JedisSentinelPool jedisSentinelPool;

    private final Configuration parameters;

    public RedisSyncClient(Configuration parameters) {
        this.parameters = parameters;
    }

    private Jedis getJedisInner() {
        JedisPoolConfig poolConfig = getConfig();
        String[] nodes = StringUtils.split(this.parameters.get(ConfigKeys.redis_addresses), ",");

        int redisTimeOut = this.parameters.get(ConfigKeys.redis_timeout);
        String redisPassword = this.parameters.get(ConfigKeys.redis_password);
        int redisDatabase = this.parameters.get(ConfigKeys.redis_db);
        Pattern ipPattern = Pattern.compile("(?<host>(.*)):(?<port>\\d+)*");
        String rct = this.parameters.get(ConfigKeys.redis_connect_type); // STANDALONE SENTINEL CLUSTER
        Jedis jedis;
        switch (rct) {
            case "STANDALONE":
                String firstIp = null;
                String firstPort = null;
                Matcher standalone = ipPattern.matcher(nodes[0]);
                if (standalone.find()) {
                    firstIp = standalone.group("host").trim();
                    firstPort = standalone.group("port").trim();
                    firstPort = firstPort == null ? "6379" : firstPort;
                }
                if (Objects.nonNull(firstIp) && pool == null) {
                    pool =
                            new JedisPool(
                                    poolConfig,
                                    firstIp,
                                    Integer.parseInt(firstPort),
                                    redisTimeOut,
                                    redisPassword,
                                    redisDatabase);
                } else {
                    throw new IllegalArgumentException(
                            String.format("redis url error. current url [%s]", nodes[0]));
                }

                jedis = pool.getResource();
                break;
            case "SENTINEL":
                Set<String> ipPorts = new HashSet<>(Arrays.asList(nodes));
                if (jedisSentinelPool == null) {
                    jedisSentinelPool =
                            new JedisSentinelPool(
                                    "",// redisConf.getMasterName()
                                    ipPorts,
                                    poolConfig,
                                    redisTimeOut,
                                    redisPassword,
                                    redisDatabase);
                }
                jedis = jedisSentinelPool.getResource();
                break;
//            case "CLUSTER":
//                Set<HostAndPort> addresses = new HashSet<>();
//                // 对ipv6 支持
//                for (String node : nodes) {
//
//                    Matcher matcher = ipPattern.matcher(node);
//                    if (matcher.find()) {
//                        String host = matcher.group("host").trim();
//                        String portStr = matcher.group("port").trim();
//                        if (StringUtils.isNotBlank(host) && StringUtils.isNotBlank(portStr)) {
//                            // 转化为int格式的端口
//                            int port = Integer.parseInt(portStr);
//                            addresses.add(new HostAndPort(host, port));
//                        }
//                    }
//                }
//                jedis =
//                        new JedisCluster(
//                                addresses,
//                                redisTimeOut,
//                                redisTimeOut,
//                                10,
//                                redisPassword,
//                                poolConfig);
//                break;
            default:
                throw new IllegalArgumentException(
                        "unsupported redis type[ " + rct + "]");
        }

        return jedis;
    }

    /**
     * test jedis client whether is in active or not,if not,close and get a new jedis client
     *
     * @param jedis
     * @param key
     * @return
     * @throws IOException
     */
    public Jedis testTimeout(Jedis jedis, String key) throws IOException {
        try {
            jedis.exists(key);
            return jedis;
        } catch (JedisConnectionException e) {
            if (jedis != null) {
                closeJedis(jedis);
            }
            return pool.getResource();
        }
    }

    public Jedis getJedis() {
        Jedis jedisInner = null;
        for (int i = 0; i <= 2; i++) {
            try {
                LOG.info("connect " + (i + 1) + " times.");
                jedisInner = getJedisInner();
                if (jedisInner != null) {
                    LOG.info("jedis is connected = {} ", jedisInner);
                    break;
                }
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                LOG.error(
                        "connect failed:{} , sleep 3 seconds reconnect",
                        e.getMessage());
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException interruptedException) {
                    throw new RuntimeException(interruptedException);
                }
                if (i == 2) {
                    throw new RuntimeException(e);
                }
            }
        }
        return jedisInner;
    }

    public void closeJedis(Jedis jedis) {
        try {
            if (jedis != null) {
                if (jedis instanceof Closeable) {
                    ((Closeable) jedis).close();
                }
            }
        } catch (Exception e) {
            LOG.error("close jedis error", e);
        }
    }

    public void close(Jedis jedis) {
        try {
            if (jedis != null && jedis.isConnected()) {
                ((Closeable) jedis).close();
            }
            if (jedisSentinelPool != null) {
                jedisSentinelPool.close();
            }
            if (pool != null) {
                pool.close();
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    private JedisPoolConfig getConfig() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(this.parameters.get(ConfigKeys.redis_max_total));// redisConf.getMaxTotal()
        jedisPoolConfig.setMaxIdle(this.parameters.get(ConfigKeys.redis_max_idle));// redisConf.getMaxIdle()
        jedisPoolConfig.setMinIdle(this.parameters.get(ConfigKeys.redis_min_idle));// redisConf.getMinIdle()
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setTestOnReturn(true);
        return jedisPoolConfig;
    }
}
