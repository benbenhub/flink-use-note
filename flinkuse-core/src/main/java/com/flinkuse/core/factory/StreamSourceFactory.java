package com.flinkuse.core.factory;

import com.flinkuse.core.base.ConfigBase;
import com.flinkuse.core.connector.cdc.FlinkcdcConnectorFormat;
import com.flinkuse.core.connector.cos.CosInputFormat;
import com.flinkuse.core.constance.ConfigKeys;
import com.flinkuse.core.deserializer.BinlogDeserializer;
import com.flinkuse.core.deserializer.OplogDeserializer;
import com.flinkuse.core.modul.BinlogBean;
import com.flinkuse.core.modul.OplogBean;
import com.flinkuse.core.util.DateTimeUtils;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.mongodb.source.MongoSourceBuilder;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class StreamSourceFactory extends ConfigBase {

    private static StreamExecutionEnvironment env;

    public StreamSourceFactory(Configuration scpsConfig, StreamExecutionEnvironment env) {
        super(scpsConfig);
        StreamSourceFactory.env = env;
    }

    public DataStream<BinlogBean> mysqlCdcSource(StartupOptions ss) {
        /*
        flink cdc改成新的api
        return env.addSource(new FlinkcdcConnectorFormat(this.scpsConfig).createMysqlCdc(ss, new BinlogDeserializerMap()));
         */
        return env.fromSource(new FlinkcdcConnectorFormat(this.scpsConfig).createMysqlCdcNew(ss, new BinlogDeserializer())
            , WatermarkStrategy.<BinlogBean>forMonotonousTimestamps().withTimestampAssigner(
                (SerializableTimestampAssigner<BinlogBean>) (ob, l) -> {
                    if (ob.getTSMS() == null || ob.getTSMS() == 0) {
                        return DateTimeUtils.getTimeStamp(); // 获取系统时间戳
                    } else {
                        return ob.getTSMS();
                    }
                }
            )
            , "scps binlog source");
    }

    public <T> DataStream<T> kafkaSource(KafkaSource<T> source, WatermarkStrategy<T> wm) {
        return env.fromSource(source, wm, "scps kafka source");
    }

    public DataStream<OplogBean> mongodbCdcSource() {
        return env.fromSource(
            new FlinkcdcConnectorFormat(this.scpsConfig).createMongodbCdc(new OplogDeserializer())
            , WatermarkStrategy.<OplogBean>forMonotonousTimestamps().withTimestampAssigner(
                (SerializableTimestampAssigner<OplogBean>) (ob, l) -> {
                    if (ob.getTSMS() == null || ob.getTSMS() == 0) {
                        return DateTimeUtils.getTimeStamp();//获取系统时间戳
                    } else {
                        return ob.getTSMS();
                    }
                }
            )
            , "scps oplog source");
    }

    public DataStream<Map<String, BsonValue>> mongodbSource(MongoSourceBuilder<Map<String, BsonValue>> msb) {

        String encodedPassword = URLEncoder.encode(this.scpsConfig.get(ConfigKeys.mongodb_password), StandardCharsets.UTF_8);

        String ur = this.scpsConfig.get(ConfigKeys.mongodb_username) + ":" + encodedPassword + "@";
        String url = this.scpsConfig.get(ConfigKeys.mongodb_host) + ":" + this.scpsConfig.get(ConfigKeys.mongodb_port);
        if (ur.length() < 3) ur = "";
        return env.fromSource(msb.setUri(String.format("mongodb://%s%s", ur, url)
        ).setDeserializationSchema(new MongoDeserializationSchema<Map<String, BsonValue>>() {
            @Override
            public Map<String, BsonValue> deserialize(BsonDocument bsonDocument) throws IOException {
                Map<String, BsonValue> result = new HashMap<>();
                bsonDocument.forEach(result::put);
                return result;
            }

            @Override
            public TypeInformation<Map<String, BsonValue>> getProducedType() {
                return new MapTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO,TypeInformation.of(BsonValue.class));
            }
        }).build(), WatermarkStrategy.noWatermarks(), "scps MongoDB Source");
    }

    public DataStream<String> cosSource(String... cosObjs) {
        return env.createInput(new CosInputFormat(cosObjs));
    }

}
