package com.flinkuse.cases.connector;

import com.flinkuse.core.base.BatchApp;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Map;

/**
 * @author learn
 * @date 2024/2/19 11:11
 */
public class ElasticsearchSource extends BatchApp {

    public ElasticsearchSource(String[] args) {
        super(args);
    }

    @Override
    public void run(ExecutionEnvironment batchEnv) {

        try {
            String[] includes = new String[]{"remote_addr","sopeiHost","user_id","url"};

            DataSet<Map<String,Object>> source = source(
            ).elasticsearchSource(
                    "indexName",
                    QueryBuilders.boolQuery().should(QueryBuilders.existsQuery("user_id")).toString(),
                    includes);

            source.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
