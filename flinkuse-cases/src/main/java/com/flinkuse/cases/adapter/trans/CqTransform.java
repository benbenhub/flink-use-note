package com.flinkuse.cases.adapter.trans;

import com.flinkuse.cases.adapter.constant.CqFinal;
import com.flinkuse.cases.adapter.entity.IdDataEntity;
import com.flinkuse.cases.common.AdapterEsEntity;
import com.flinkuse.cases.common.CountAndTimeTrigger;
import com.flinkuse.core.base.StreamApp;
import com.flinkuse.core.constance.ConfigKeys;
import com.flinkuse.core.deserializer.KafkaDeserializerBinlog;
import com.flinkuse.core.enums.OdsBasicsConf;
import com.flinkuse.core.modul.BinlogBean;
import com.flinkuse.core.util.DateTimeUtils;
import com.flinkuse.core.util.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchEmitter;
import org.apache.flink.connector.elasticsearch.sink.RequestIndexer;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 设置一个五分钟为周期的窗口收集主要的id信息
 * 运用收集上来的信息把全部的数据查询上来
 * 运用收集上来的信息把es的数据查询上来,作为后续的删除数据
 * 匹配查询上来的数据,构造es结构
 * 操作es
 * @author learn
 * @date 2023/5/31 9:31
 */
public abstract class CqTransform extends StreamApp {

    protected Map<String, Integer> oemTables;
    protected Map<String, Integer> skuTables;
    protected long windowsMinutes;
    protected long windowsSeconds;
    protected String offset;
    protected String odsTopicGroup;
    protected long windowsCount;
    protected String oemIndex;
    protected String stdIndex;

    private final String oemIdKey = "oem_partsku_id";
    private final String skuIdKey = "ten_partsku_id";

    public CqTransform(String[] args, String jobName) {
        super(args, jobName);
    }
    @Override
    public void run(StreamExecutionEnvironment streamEnv) {
        argsAssignment();

        // 数据源 选出涉及到任务的表
        DataStream<IdDataEntity> source = sourceFilterTables();

        // 实时同步sku关系
        skuSync(shuntDistinct(source, skuIdKey));
        // 实时同步oem等基础数据关系
        oemSync(shuntDistinct(source, oemIdKey));
    }

    public void skuSync(DataStream<String> source) {
        // 查询数据
        DataStream<AdapterEsEntity> sourceFindData = skuFindData(source);

        // 查找es将要删除的数据
        DataStream<AdapterEsEntity> sourceFindEsDeleteData = skuFindEsDeleteData(source);

        // 合并删除增加的数据 设置数据输出顺序
        DataStream<AdapterEsEntity> tranEsSinkResult = operationSequence(sourceFindEsDeleteData.union(sourceFindData));

        // 数据输出
        skuAdapterSink(tranEsSinkResult);
    }
    public void oemSync(DataStream<String> source) {
        // 查询数据
        DataStream<AdapterEsEntity> sourceFindData = oemFindData(source);

        // 查找es将要删除的数据
        DataStream<AdapterEsEntity> sourceFindEsDeleteData = oemFindEsDeleteData(source);

        // 合并删除增加的数据 设置数据输出顺序
        DataStream<AdapterEsEntity> tranEsSinkResult = operationSequence(sourceFindEsDeleteData.union(sourceFindData));

        // 数据输出
        oemAdapterSink(tranEsSinkResult);
    }

    private DataStream<String> shuntDistinct(DataStream<IdDataEntity> source, String key) {
        return source.flatMap(new FlatMapFunction<IdDataEntity, Long>() {
            @Override
            public void flatMap(IdDataEntity value, Collector<Long> out) throws Exception {
                if (value.getKey().equals(key)) {
                    out.collect(value.getId());
                }
            }
        }).windowAll(TumblingEventTimeWindows.of(Time.minutes(windowsMinutes))
        ).process(new ProcessAllWindowFunction<Long, Long, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<Long> e, Collector<Long> out) throws Exception {
                Set<Long> set = new HashSet<>();
                e.forEach(set::add);
                set.forEach(out::collect);
            }
        }).name(key + "shunt distinct"
        ).windowAll(GlobalWindows.create()
        ).trigger(new CountAndTimeTrigger<>(windowsCount, Time.seconds(windowsSeconds))
        ).process(new ProcessAllWindowFunction<Long, String, GlobalWindow>() {
            @Override
            public void process(Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
                StringBuilder o = new StringBuilder();
                elements.forEach(e -> o.append(e).append(","));
                if (o.length() > 1)
                    o.delete(o.length() - 1, o.length());
                out.collect(o.toString());
            }
        }).name(key + "id count batch");
    }

    public abstract void skuDeleteIndexer(Object data, RequestIndexer indexer, long wm);
    public abstract void skuInsertIndexer(Object data, RequestIndexer indexer, long wm);
    public abstract DataStream<AdapterEsEntity> skuFindData(DataStream<String> sourceFindIds);
    public abstract DataStream<AdapterEsEntity> skuFindEsDeleteData(DataStream<String> sourceFindIds);

    public abstract void oemDeleteIndexer(Object data, RequestIndexer indexer, long wm);
    public abstract void oemInsertIndexer(Object data, RequestIndexer indexer, long wm);
    public abstract DataStream<AdapterEsEntity> oemFindData(DataStream<String> sourceFindIds);
    public abstract DataStream<AdapterEsEntity> oemFindEsDeleteData(DataStream<String> sourceFindIds);

    private void oemAdapterSink(DataStream<AdapterEsEntity> tranEsSinkResult) {
        sink().elasticsearchSink(tranEsSinkResult, new ElasticsearchEmitter<AdapterEsEntity>() {
            @Override
            public void emit(AdapterEsEntity e, SinkWriter.Context c, RequestIndexer indexer) {
                switch (e.getAction()) {
                    case delete:
                        oemDeleteIndexer(e.getData(), indexer, c.currentWatermark());
                        break;
                    case insert:
                        oemInsertIndexer(e.getData(), indexer, c.currentWatermark());
                        break;
                }
            }
        });
    }

    private void skuAdapterSink(DataStream<AdapterEsEntity> tranEsSinkResult) {
        sink().elasticsearchSink(tranEsSinkResult, new ElasticsearchEmitter<AdapterEsEntity>() {
            @Override
            public void emit(AdapterEsEntity e, SinkWriter.Context c, RequestIndexer indexer) {
                switch (e.getAction()) {
                    case delete:
                        skuDeleteIndexer(e.getData(), indexer, c.currentWatermark());
                        break;
                    case insert:
                        skuInsertIndexer(e.getData(), indexer, c.currentWatermark());
                        break;
                }
            }
        });
    }

    public DataStream<IdDataEntity> sourceFilterTables() {

        return source().kafkaSource(KafkaSource.<BinlogBean>builder()
                .setBootstrapServers(getScpsConfig().get(ConfigKeys.kafka_bootstrap_servers))
                .setTopics(OdsBasicsConf.odsTopic)
                .setGroupId(odsTopicGroup)
                .setStartingOffsets(StringUtils.toKafkaOffset(offset)) // 从最新的开始
                .setValueOnlyDeserializer(new KafkaDeserializerBinlog())
                .build()
        , WatermarkStrategy.<BinlogBean>forMonotonousTimestamps().withTimestampAssigner(
                (SerializableTimestampAssigner<BinlogBean>) (ob, l) -> {
                    if (ob.getTSMS() == null || ob.getTSMS() == 0) {
                        return DateTimeUtils.getTimeStamp(); // 获取系统时间戳
                    } else {
                        return ob.getTSMS();
                    }
                }
        )
        // 过滤掉不需要的表数据
        ).flatMap(new FlatMapFunction<BinlogBean, IdDataEntity>() {
            @Override
            public void flatMap(BinlogBean f, Collector<IdDataEntity> out) throws Exception {
                if (oemTables.containsKey(f.getTableName())
                        || skuTables.containsKey(f.getTableName())) {
                    Map<String,Object> data;
                    switch (f.getOperationType()) {
                        case delete:
                            data = f.getDataBefore();
                            break;
                        case insert:
                        case update:
                        default:
                            data = f.getDataAfter();
                            break;
                    }
                    IdDataEntity id = new IdDataEntity();
                    if (data.containsKey(oemIdKey)) {
                        id.setKey(oemIdKey);
                        id.setId(Long.valueOf(data.get(oemIdKey).toString()));
                        out.collect(id);
                    }
                    if (data.containsKey(skuIdKey)) {
                        id.setKey(skuIdKey);
                        id.setId(Long.valueOf(data.get(skuIdKey).toString()));
                        out.collect(id);
                    }
                }
            }
        }).name("source adapter filter");
    }

    public void argsAssignment() {
        oemTables = new HashMap<>();
        skuTables = new HashMap<>();
        for (String t : CqFinal.cmOemStdTables.split(",")) {
            oemTables.put(t, 1);
        }
        for (String t : CqFinal.skuTables.split(",")) {
            skuTables.put(t, 1);
        }
    }

    public DataStream<AdapterEsEntity> operationSequence(DataStream<AdapterEsEntity> ss) {
        return ss.flatMap(new FlatMapFunction<AdapterEsEntity, AdapterEsEntity>() {
            @Override
            public void flatMap(AdapterEsEntity v, Collector<AdapterEsEntity> out) throws Exception {
                // 给数据排序 delete类型减1秒insert类型增加一分钟
                int cc = 1000 * 60;
                switch (v.getAction()) {
                    case delete:
                        v.setWatermark(DateTimeUtils.getTimeStamp() - cc);
                        break;
                    case insert:
                        v.setWatermark(DateTimeUtils.getTimeStamp() + cc);
                        break;
                }
                out.collect(v);
            }
        }).name("operation sequence"
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<AdapterEsEntity>forMonotonousTimestamps().withTimestampAssigner(
                (SerializableTimestampAssigner<AdapterEsEntity>) (ob, l) -> ob.getWatermark()
        )).name("operation watermark");
    }
}
