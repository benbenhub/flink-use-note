package com.flinkuse.cases.connector;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flinkuse.core.base.StreamApp;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.BsonDocument;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

/**
 * @author learn
 * @date 2024/3/22 15:59
 */
public class MongodbSyncES extends StreamApp {

    private final String ctrl;
    private final String indexPrefix;
    private final String indexSuffix;
    private final String batchFlag;

    public MongodbSyncES(String[] args, String jobName) {
        super(args, jobName);
        ctrl = getScpsParams().get("epc_ctrl", "3");
        indexPrefix = getScpsParams().get("epc_index_prefix", "");
        indexSuffix = getScpsParams().get("epc_index_suffix", "test");
        batchFlag = getScpsParams().get("epc_batch_flag", "batch");
    }
    public static void main(String[] args) throws Exception {
        new MongodbSyncES(args, "epc").start();
    }
    @Override
    public void run(StreamExecutionEnvironment streamEnv) {

        switch (batchFlag) {
            case "batch":
                for (String c : ctrl.split(",")) {
                    switch (c) {
                        case "1":
                            batchSync("t_a");
                            break;
                        case "2":
                            batchSync("t_o");
                            break;
                        case "3":
                            batchSync("t_p");
                            break;
                        case "4":
                            batchSync("t_pa");
                            break;
                        case "5":
                            batchSync("t_par");
                            break;
                    }
                }
                break;
            case "stream":
                streamSync();
                break;
        }
    }

    private void batchSync(String tb) {
        sink().elasticsearchSink(source().mongodbSource(
                MongoSource.<String>builder(
                ).setDatabase("bmw"
                ).setCollection(tb
                ).setDeserializationSchema(
                        new MongoDeserializationSchema<>() {
                            @Override
                            public String deserialize(BsonDocument bsonDocument) throws IOException {
                                return bsonDocument.toJson();
                            }
                            @Override
                            public TypeInformation<String> getProducedType() {
                                return BasicTypeInfo.STRING_TYPE_INFO;
                            }
                        }
                )),
                (element, context, indexer) -> {
                    Map<String, Object> data = JSON.parseObject(element);
                    String _id = ((JSONObject) data.get("_id")).get("$oid").toString();
                    data.remove("_id");
                    indexer.add(
                            Requests.indexRequest()
                                    .index(indexPrefix + tb + indexSuffix)
                                    .id(_id)
                                    .source(data, XContentType.JSON));
                });
    }

    private void streamSync() {
        sink().elasticsearchSink(source().mongodbCdcSource() ,
                (element, context, indexer) -> {
                    switch (element.getOperationType()) {
                        case insert:
                        case update:
                            indexer.add(
                                    Requests.indexRequest()
                                            .index(indexPrefix + element.getCollection() + indexSuffix)
                                            .id(element.getDocumentKey())
                                            .source(element.getFullDocument(), XContentType.JSON));
                            break;
                        case delete:
                            indexer.add(
                                    Requests.deleteRequest(indexPrefix + element.getCollection() + indexSuffix)
                                            .id(element.getDocumentKey()));
                            break;
                    }
                });
    }
}
