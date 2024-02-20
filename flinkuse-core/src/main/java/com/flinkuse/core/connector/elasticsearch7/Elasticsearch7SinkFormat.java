package com.flinkuse.core.connector.elasticsearch7;

import com.flinkuse.core.base.ConfigBase;
import com.flinkuse.core.constance.ConfigKeys;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchEmitter;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.http.HttpHost;

import java.util.ArrayList;
import java.util.List;

/**
 * @author learn
 * @date 2023/2/1 11:04
 */
public class Elasticsearch7SinkFormat extends ConfigBase {
    public Elasticsearch7SinkFormat(Configuration scpsConfig) {
        super(scpsConfig);
    }
    public <T> ElasticsearchSink<T> createEs7Sink(ElasticsearchEmitter<? super T> emitter) {
        String[]  hostStr = this.scpsConfig.get(ConfigKeys.elasticsearch_hosts).split(",");
        List<HttpHost> hosts = new ArrayList<>();
        for (String h : hostStr) {
            String[] ip = h.split(":");
            hosts.add(new HttpHost(ip[0], Integer.parseInt(ip[1]), this.scpsConfig.get(ConfigKeys.elasticsearch_scheme)));
        }
        return new Elasticsearch7SinkBuilder<T>()
                .setBulkFlushMaxActions(this.scpsConfig.getInteger(ConfigKeys.elasticsearch_bulk_flush_max_actions))
                .setBulkFlushInterval(this.scpsConfig.getLong(ConfigKeys.elasticsearch_bulk_flush_interval))
                .setHosts(hosts.toArray(new HttpHost[0]))

                .setConnectionUsername(this.scpsConfig.get(ConfigKeys.elasticsearch_username))
                .setConnectionPassword(this.scpsConfig.get(ConfigKeys.elasticsearch_password))

                .setSocketTimeout(this.scpsConfig.getInteger(ConfigKeys.elasticsearch_socket_time_out))

                .setConnectionRequestTimeout(this.scpsConfig.getInteger(ConfigKeys.elasticsearch_connection_request_time_out))

                .setConnectionTimeout(this.scpsConfig.getInteger(ConfigKeys.elasticsearch_connect_time_out))

                .setEmitter(emitter)
                .build();
    }
}
