package com.flinkuse.cases.code;

import com.alibaba.fastjson.JSONObject;
import com.flinkuse.cases.code.sql.RealSql;
import com.github.houbb.pinyin.bs.PinyinBs;
import com.github.houbb.pinyin.constant.enums.PinyinStyleEnum;
import com.github.houbb.pinyin.support.style.PinyinToneStyles;
import com.flinkuse.cases.common.AdapterEsEntity;
import com.flinkuse.cases.common.CountAndTimeTrigger;
import com.flinkuse.core.base.StreamApp;
import com.flinkuse.core.connector.elasticsearch7.Elasticsearch7AsyncFormat;
import com.flinkuse.core.connector.jdbc.JdbcAsyncFormat;
import com.flinkuse.core.connector.jdbc.JdbcStatementFunction;
import com.flinkuse.core.constance.ConfigKeys;
import com.flinkuse.core.deserializer.KafkaDeserializerBinlog;
import com.flinkuse.core.enums.JdbcConnectionType;
import com.flinkuse.core.enums.OdsBasicsConf;
import com.flinkuse.core.enums.SqlOperate;
import com.flinkuse.core.modul.BinlogBean;
import com.flinkuse.core.util.DateTimeUtils;
import com.flinkuse.core.util.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchEmitter;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author learn
 * @date 2023/5/31 16:45
 */
public class CodeSyncEs extends StreamApp {

    private final Long windowsMinutes;
    private final String offset;
    private final String esIndex;
    private final String odsTopicGroup;
    private final int capacity;
    private final long windowsCount;
    private final long windowsSeconds;

    public static void main(String[] args) {
        try {
            new CodeSyncEs(args, "code sync es").start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public CodeSyncEs(String[] args, String jobName) {
        super(args, jobName);
        windowsMinutes = getScpsParams().getLong("code_windows_time");
        esIndex = getScpsParams().get("code_es_index");
        odsTopicGroup = "code-ods-topic-group";
        offset = getScpsParams().get("code_offset", "latest");
        capacity = getScpsParams().getInt("code_capacity", 5);
        windowsCount = getScpsParams().getInt("code_windows_count", 100);
        windowsSeconds = getScpsParams().getInt("cq_windows_seconds", 10);
    }

    @Override
    public void run(StreamExecutionEnvironment streamEnv) {
        // 筛选数据源
        DataStream<BinlogBean> source = sourceFilterTables();

        //runCmMain(source.filter((FilterFunction<BinlogBean>) b -> b.getTableName().equals("t_carmodel_base")));

        runMain(source.filter((FilterFunction<BinlogBean>) b ->
                b.getTableName().equals("t_dealer_partsku")
        ), "dealer_partsku_id", RealSql.ch_productSql);

        runMain(source.filter((FilterFunction<BinlogBean>) b ->
                b.getTableName().equals("t_famous_partsku") || b.getTableName().equals("t_famous_partsku_code")
        ), "fms_partsku_id", RealSql.ch_fmsSql);

        runMain(source.filter((FilterFunction<BinlogBean>) b ->
                b.getTableName().equals("t_industry_partsku") || b.getTableName().equals("t_industry_partsku_code")
        ), "indus_partsku_id", RealSql.ch_insSql);

        runMain(source.filter((FilterFunction<BinlogBean>) b ->
                b.getTableName().equals("t_oem_partsku") || b.getTableName().equals("t_oem_partsku_code")
        ), "oem_partsku_id", RealSql.ch_oemSql);

        runMain(source.filter((FilterFunction<BinlogBean>) b ->
                b.getTableName().equals("t_product_sku")
        ), "prod_sku_id", RealSql.ch_dealerSql);

        runMain(source.filter((FilterFunction<BinlogBean>) b ->
                b.getTableName().equals("t_tenant_partsku")
        ), "ten_partsku_id", RealSql.ch_skuSql);

        runMain(source.filter((FilterFunction<BinlogBean>) b ->
                b.getTableName().equals("t_tenant_partsku_exchange")
        ), "partsku_exchange_id", RealSql.ch_exchangeSql);
    }

    /**
     * 主逻辑函数
     * 用esQueryKey识别不同编码类型 有的进行特殊处理
     * @param source 数据源
     * @param esQueryKey 不同的编码类型数据操作的粒度,多数为主键
     * @param queryDBSql 查询数据的sql
     */
    public void runMain(DataStream<BinlogBean> source
            , String esQueryKey
            , String queryDBSql) {

        DataStream<String> ids = source.windowAll(TumblingEventTimeWindows.of(Time.minutes(windowsMinutes))
        ).process(new ProcessAllWindowFunction<BinlogBean, String, TimeWindow>() {
            @Override
            public void process(Context c, Iterable<BinlogBean> i, Collector<String> out) throws Exception {
                List<String> idStr = new ArrayList<>();
                for (BinlogBean b : i) {
                    Map<String, Object> data;
                    switch (b.getOperationType()) {
                        case delete:
                            data = b.getDataBefore();
                            break;
                        case insert:
                        case update:
                        default:
                            data = b.getDataAfter();
                            break;
                    }
                    idStr.add(Objects.toString(data.get(esQueryKey), ""));
                }
                idStr = idStr.stream().distinct().collect(Collectors.toList());
                for (String s : idStr) {
                    out.collect(s);
                }
            }
        }).name(esQueryKey + " find op ids"
        ).windowAll(GlobalWindows.create() // 全窗口 聚合
        ).trigger(new CountAndTimeTrigger<>(windowsCount, Time.seconds(windowsSeconds)) // 条数时间触发器
        ).process(new ProcessAllWindowFunction<String, String, GlobalWindow>() {
            @Override
            public void process(Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                StringBuilder res = new StringBuilder();
                for (String s : elements) {
                    res.append(s).append(",");
                }
                if (res.length() > 1)
                    res.delete(res.length() - 1, res.length());

                out.collect(res.toString());
            }
        });
        // 找到要删除的es数据
        DataStream<AdapterEsEntity> delData = AsyncDataStream.orderedWait(ids
            , new Elasticsearch7AsyncFormat<String, AdapterEsEntity>() {
                @Override
                public void asyncInvoke(String in, ResultFuture<AdapterEsEntity> out) throws Exception {

                    BoolQueryBuilder bqbStdSku = new BoolQueryBuilder();
                    bqbStdSku.must(QueryBuilders.termsQuery(esQueryKey, in.split(",")));

                    List<String> stdSkuList = getFunction().searchId(esIndex, bqbStdSku);

                    AdapterEsEntity aee = new AdapterEsEntity();
                    aee.setData(stdSkuList);
                    aee.setAction(SqlOperate.delete);

                    out.complete(Collections.singleton(aee));
                }

                @Override
                public void timeout(String in, ResultFuture<AdapterEsEntity> out) {
                    log.error("查找要删除的es数据时超时:{}", in);
                    out.completeExceptionally(new RuntimeException("查找要删除的es数据时超时!"));
                }
            }
            , windowsMinutes
            , TimeUnit.MINUTES
        ).name(esQueryKey + " query es delete data");
        // 找到需要添加的数据
        DataStream<AdapterEsEntity> insData = AsyncDataStream.orderedWait(ids
            , new JdbcAsyncFormat<String, AdapterEsEntity>(JdbcConnectionType.clickhouse) {
                @Override
                public AdapterEsEntity asyncInvokeInputHandle(String i, JdbcStatementFunction q) throws Exception {
                    AdapterEsEntity aee = new AdapterEsEntity();
                    aee.setAction(SqlOperate.insert);
                    String sql = queryDBSql.replace("${"+ esQueryKey +"}", i);
                    if (esQueryKey.equals("dealer_partsku_id")) {
                        // dealer_partsku_id 时影响两种编码类型的索引
                        // 所以 查询两次数据库
                        String sql1 = RealSql.ch_dpSql.replace("${"+ esQueryKey +"}", i);
//                        log.info("=========================\n{}\n", sql);
//                        log.info("=========================\n{}\n", sql1);
                        List<Map<String,Object>> lm = q.runQuery(sql);
                        lm.addAll(q.runQuery(sql1));
                        aee.setData(lm);
                    } else {
//                        log.info("=========================\n{}\n", sql);
                        aee.setData(q.runQuery(sql));
                    }
                    return aee;
                }

                @Override
                public void timeout(String input, ResultFuture<AdapterEsEntity> resultFuture) {
                    log.error("查找数据时超时:{}", input);
                    resultFuture.completeExceptionally(new RuntimeException("查找数据时超时!"));
                }
            }
            , windowsMinutes
            , TimeUnit.MINUTES
            , capacity
        ).name(esQueryKey + " query tables");

        // 设置水印
        DataStream<AdapterEsEntity> sinkData = delData.union(insData).flatMap(new FlatMapFunction<AdapterEsEntity, AdapterEsEntity>() {
            @Override
            public void flatMap(AdapterEsEntity v, Collector<AdapterEsEntity> out) throws Exception {
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
        }).name(esQueryKey + " set watermark"
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<AdapterEsEntity>forMonotonousTimestamps(
        ).withTimestampAssigner((SerializableTimestampAssigner<AdapterEsEntity>) (ob, l) -> ob.getWatermark())
        ).name(esQueryKey + " watermark");

        // 输出到es
        sink().elasticsearchSink(sinkData, (ElasticsearchEmitter<AdapterEsEntity>) (e, c, indexer) -> {
            switch (e.getAction()) {
                case delete:
                    List<String> t2 = (List<String>) e.getData();
                    log.info("数据操作类型:{},object水印:{},current水印:{},删除{}条数据", e.getAction(), e.getWatermark(), c.currentWatermark(), t2.size());
                    for (String s : t2) {
                        indexer.add(Requests.deleteRequest(esIndex).id(s));
                    }
                    break;
                case insert:
                    List<Map<String, Object>> data = (List<Map<String, Object>>) e.getData();
                    log.info("数据操作类型:{},object水印:{},current水印:{},增加{}条数据", e.getAction(), e.getWatermark(), c.currentWatermark(), data.size());
                    for (Map<String, Object> m : data) {
                        Map<String, String> out = new HashMap<>();
                        for (Map.Entry<String, Object> entry : m.entrySet()) {
                            out.put(entry.getKey(), Objects.toString(entry.getValue(),""));
                        }
                        if ((esQueryKey.equals("prod_sku_id") || esQueryKey.equals("dealer_partsku_id"))
                                && m.containsKey("prod_sku_id")) {
                            String prodSkuNameStr = out.get("prod_sku_name").toLowerCase(Locale.ROOT);
                            String prodSkuNamePyFirst = PinyinBs
                                    .newInstance()
                                    .style(PinyinToneStyles.getTone(PinyinStyleEnum.FIRST_LETTER))
                                    .toPinyin(prodSkuNameStr)
                                    .replaceAll("[^A-Za-z0-9]","");
                            String prodSkuNamePy = PinyinBs
                                    .newInstance()
                                    .style(PinyinToneStyles.getTone(PinyinStyleEnum.NORMAL))
                                    .toPinyin(prodSkuNameStr)
                                    .replaceAll("[^A-Za-z0-9]","");

                            String prodSkuCodeStr = out.get("prod_sku_code").toLowerCase(Locale.ROOT);
                            String prodSkuCodePyFirst = PinyinBs
                                    .newInstance()
                                    .style(PinyinToneStyles.getTone(PinyinStyleEnum.FIRST_LETTER))
                                    .toPinyin(prodSkuCodeStr)
                                    .replaceAll("[^A-Za-z0-9]","");
                            String prodSkuCodePy = PinyinBs
                                    .newInstance()
                                    .style(PinyinToneStyles.getTone(PinyinStyleEnum.NORMAL))
                                    .toPinyin(prodSkuCodeStr)
                                    .replaceAll("[^A-Za-z0-9]","");

                            out.put("prod_sku_code_py_first",prodSkuCodePyFirst);
                            out.put("prod_sku_code_py",prodSkuCodePy);
                            out.put("prod_sku_name_py_first",prodSkuNamePyFirst);
                            out.put("prod_sku_name_py",prodSkuNamePy);
                        }
                        out.put("watermark", Objects.toString(c.currentWatermark(),""));
                        indexer.add(Requests.indexRequest()
                                .index(esIndex)
                                .source(JSONObject.toJSONString(out), XContentType.JSON));
                    }
                    break;
            }
        });
    }

    public void runCmMain(DataStream<BinlogBean> source) {

        DataStream<Tuple2<String, String>> ids = source.windowAll(TumblingEventTimeWindows.of(Time.minutes(windowsMinutes))
        ).process(new ProcessAllWindowFunction<BinlogBean, Tuple2<String, String>, TimeWindow>() {
            @Override
            public void process(Context c, Iterable<BinlogBean> i, Collector<Tuple2<String, String>> out) throws Exception {
                StringBuilder res = new StringBuilder();
                StringBuilder engres = new StringBuilder();
                List<String> brandStr = new ArrayList<>();
                List<String> engStr = new ArrayList<>();
                for (BinlogBean b : i) {
                    Map<String, Object> data;
                    switch (b.getOperationType()) {
                        case delete:
                            data = b.getDataBefore();
                            break;
                        case insert:
                        case update:
                        default:
                            data = b.getDataAfter();
                            break;
                    }
                    brandStr.add(Objects.toString(data.get("cm_brand"), ""));
                    engStr.add(Objects.toString(data.get("cm_engine_model"), ""));
                }
                brandStr = brandStr.stream().distinct().collect(Collectors.toList());
                engStr = engStr.stream().distinct().collect(Collectors.toList());
                for (String s : brandStr) {
                    res.append(s).append(",");
                }
                if (res.length() > 1)
                    res.delete(res.length() - 1, res.length());

                for (String s : engStr) {
                    engres.append(s).append(",");
                }
                if (engres.length() > 1)
                    engres.delete(engres.length() - 1, engres.length());

                out.collect(Tuple2.of(res.toString(), engres.toString()));
            }
        }).name("find op ids");
        // 找到要删除的es数据
        DataStream<AdapterEsEntity> delData = AsyncDataStream.orderedWait(ids
            , new Elasticsearch7AsyncFormat<Tuple2<String, String>, AdapterEsEntity>() {
                @Override
                public void asyncInvoke(Tuple2<String, String> in, ResultFuture<AdapterEsEntity> out) throws Exception {

                    BoolQueryBuilder bqbStdSku = new BoolQueryBuilder();
                    bqbStdSku.must(QueryBuilders.termsQuery("cm_brand", in.f0.split(",")));
                    bqbStdSku.must(QueryBuilders.termsQuery("cm_engine_model", in.f1.split(",")));

                    List<String> stdSkuList = getFunction().searchId(esIndex, bqbStdSku);

                    AdapterEsEntity aee = new AdapterEsEntity();
                    aee.setData(stdSkuList);
                    aee.setAction(SqlOperate.delete);

                    out.complete(Collections.singleton(aee));
                }

                @Override
                public void timeout(Tuple2<String, String> in, ResultFuture<AdapterEsEntity> out) {
                    log.error("查找要删除的es数据时超时:{}", in);
                    out.completeExceptionally(new RuntimeException("查找要删除的es数据时超时!"));
                }
            }
            , windowsMinutes
            , TimeUnit.MINUTES
        ).name("query es delete data");
        // 找到需要添加的数据
        DataStream<AdapterEsEntity> insData = AsyncDataStream.orderedWait(ids
            , new JdbcAsyncFormat<Tuple2<String, String>, AdapterEsEntity>(JdbcConnectionType.clickhouse) {
                @Override
                public AdapterEsEntity asyncInvokeInputHandle(Tuple2<String, String> i, JdbcStatementFunction q) throws Exception {
                    AdapterEsEntity aee = new AdapterEsEntity();
                    aee.setAction(SqlOperate.insert);
                    aee.setData(q.runQuery(String.format(RealSql.ch_cmQuerySql, i.f0, i.f1)));
                    return aee;
                }

                @Override
                public void timeout(Tuple2<String, String> input, ResultFuture<AdapterEsEntity> resultFuture) {
                    log.error("查找数据时超时:{}", input);
                    resultFuture.completeExceptionally(new RuntimeException("查找数据时超时!"));
                }
            }
            , windowsMinutes
            , TimeUnit.MINUTES
            , capacity
        ).name("query tables");

        // 设置水印
        DataStream<AdapterEsEntity> sinkData = delData.union(insData).flatMap(new FlatMapFunction<AdapterEsEntity, AdapterEsEntity>() {
            @Override
            public void flatMap(AdapterEsEntity v, Collector<AdapterEsEntity> out) throws Exception {
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
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<AdapterEsEntity>forMonotonousTimestamps(
        ).withTimestampAssigner((SerializableTimestampAssigner<AdapterEsEntity>) (ob, l) -> ob.getWatermark()));

        // 输出到es
        sink().elasticsearchSink(sinkData, (ElasticsearchEmitter<AdapterEsEntity>) (e, c, indexer) -> {
            switch (e.getAction()) {
                case delete:
                    List<String> t2 = (List<String>) e.getData();
                    log.info("数据操作类型:{},object水印:{},current水印:{},删除{}条数据", e.getAction(), e.getWatermark(), c.currentWatermark(), t2.size());
                    for (String s : t2) {
                        indexer.add(Requests.deleteRequest(esIndex).id(s));
                    }
                    break;
                case insert:
                    List<Map<String, Object>> data = (List<Map<String, Object>>) e.getData();
                    log.info("数据操作类型:{},object水印:{},current水印:{},增加{}条数据", e.getAction(), e.getWatermark(), c.currentWatermark(), data.size());
                    for (Map<String, Object> m : data) {
                        Map<String, Object> out = new HashMap<>();
                        for (Map.Entry<String, Object> entry : m.entrySet()) {
                            if (entry.getKey().equals("cm_ids"))
                                out.put(entry.getKey(), Objects.toString(entry.getValue(),"").split(","));
                            else
                                out.put(entry.getKey(), Objects.toString(entry.getValue(),""));
                        }

                        out.put("watermark", Objects.toString(c.currentWatermark(),""));
                        indexer.add(Requests.indexRequest()
                                .index(esIndex)
                                .source(JSONObject.toJSONString(out), XContentType.JSON));
                    }
                    break;
            }
        });
    }

    public DataStream<BinlogBean> sourceFilterTables() {

        return source().kafkaSource(KafkaSource.<BinlogBean>builder()
                        .setBootstrapServers(getScpsConfig().get(ConfigKeys.kafka_bootstrap_servers))
                        .setTopics(OdsBasicsConf.odsTopic)
                        .setGroupId(odsTopicGroup)
                        .setStartingOffsets(StringUtils.toKafkaOffset(offset))
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
        ).filter((FilterFunction<BinlogBean>) f ->
                f.getTableName().equals("t_dealer_partsku")
                || f.getTableName().equals("t_famous_partsku")
                || f.getTableName().equals("t_famous_partsku_code")
                || f.getTableName().equals("t_industry_partsku")
                || f.getTableName().equals("t_industry_partsku_code")
                || f.getTableName().equals("t_oem_partsku")
                || f.getTableName().equals("t_oem_partsku_code")
                || f.getTableName().equals("t_product_sku")
                || f.getTableName().equals("t_tenant_partsku")
                || f.getTableName().equals("t_tenant_partsku_exchange")
        ).name("source adapter filter");
    }
}
