package com.flinkuse.cases.connector;

import com.alibaba.fastjson.JSONObject;
import com.flinkuse.core.base.StreamApp;
import com.flinkuse.core.connector.jdbc.JdbcAsyncFormat;
import com.flinkuse.core.connector.jdbc.JdbcStatement;
import com.flinkuse.core.constance.ConfigKeys;
import com.flinkuse.core.deserializer.KafkaDeserializerBinlog;
import com.flinkuse.core.enums.JdbcType;
import com.flinkuse.core.enums.OdsBasicsConf;
import com.flinkuse.core.enums.SqlOperate;
import com.flinkuse.core.modul.BinlogBean;
import com.flinkuse.core.serialization.KafkaSerializationBinlog;
import com.flinkuse.core.util.DateTimeUtils;
import com.flinkuse.core.util.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author learn
 * @date 2023/6/6 15:48
 */
public class KafkaOutClickhouse extends StreamApp {

    private final long asyncTimeout;
    private final int asyncOpMaxNum;
    private final long windowMilliseconds;
    private final String offset;

    public static void main(String[] args) {
        try {
            new KafkaOutClickhouse(args,"binlog ods operate").start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public KafkaOutClickhouse(String[] args, String jobName) {
        super(args, jobName);
        asyncTimeout = getScpsParams().getLong("ods_op_async_timeout", 1L);
        asyncOpMaxNum = getScpsParams().getInt("ods_op_async_MaxNum", 5);
        windowMilliseconds = getScpsParams().getLong("ods_op_window_milliseconds", 50L);
        offset = getScpsParams().get("ods_op_offset", "latest");
    }

    @Override
    public void run(StreamExecutionEnvironment streamEnv) {
        // 数据源
        DataStream<Tuple2<Long, Iterable<BinlogBean>>> source = getSource();

        // doris ods sync;ch ods sync
        // 使用join方式确保sql都已经执行完毕
//        DataStream<BinlogBean> ds = sinkChOds(source
//        ).join(sinkDorisOds(source)
//        ).where(w -> w.f0
//        ).equalTo(l -> l
//        ).window(SlidingEventTimeWindows.of(Time.milliseconds(windowMilliseconds), Time.milliseconds(windowMilliseconds))
//        ).trigger(EventTimeTrigger.create()
//        ).apply(new FlatJoinFunction<Tuple2<Long, Iterable<BinlogBean>>, Long, BinlogBean>() {
//            @Override
//            public void join(Tuple2<Long, Iterable<BinlogBean>> v, Long second, Collector<BinlogBean> out) throws Exception {
//                v.f1.forEach(out::collect);
//            }
//        });
        DataStream<BinlogBean> ds = sinkChOds(source
        ).flatMap(new FlatMapFunction<Tuple2<Long, Iterable<BinlogBean>>, BinlogBean>() {
            @Override
            public void flatMap(Tuple2<Long, Iterable<BinlogBean>> v, Collector<BinlogBean> out) throws Exception {
                v.f1.forEach(out::collect);
            }
        });

        // sink batch tables
//        sinkDorisOrCh(source, JdbcConnectionType.doris);
        sinkDorisOrCh(source, JdbcType.clickhouse);

        // sink kafka
        sink().kafkaSink(ds
                , KafkaRecordSerializationSchema.builder(
                ).setTopic(OdsBasicsConf.odsTopic
                ).setValueSerializationSchema(new KafkaSerializationBinlog()
                ).build());
    }

    public DataStream<Tuple2<Long, Iterable<BinlogBean>>> getSource() {
//        DataStream<BinlogBean> source = source().mysqlCdcSource(StartupOptions.timestamp(offsetTimestamp));
//
//        return source.flatMap(new FlatMapFunction<BinlogBean, BinlogBean>() {
//            @Override
//            public void flatMap(BinlogBean value, Collector<BinlogBean> out) throws Exception {
//                String tabName = value.getTableName();
//                // 把带有数字的表名的数字删掉
//                tabName = tabName.replaceAll("\\d+","");
//                // 递归删除最后的下划线
//                value.setTableName(lastUnderlineDelete(tabName));
//
//                out.collect(value);
//            }
//        });
        return source(
        ).kafkaSource(
                KafkaSource.<BinlogBean>builder(
                ).setBootstrapServers(getScpsConfig().get(ConfigKeys.kafka_bootstrap_servers)
                ).setTopics(OdsBasicsConf.binlogTopic
                ).setGroupId("realKFGid-binlog-op"
                ).setStartingOffsets(StringUtils.toKafkaOffset(offset)
                ).setValueOnlyDeserializer(new KafkaDeserializerBinlog()
                ).build()
                , WatermarkStrategy.<BinlogBean>forMonotonousTimestamps().withTimestampAssigner(
                        (SerializableTimestampAssigner<BinlogBean>) (ob, l) -> ob.getTSMS()
                )
        //).keyBy(BinlogBean::getTSMS
        //).rebalance( // 数据均衡设置
        ).filter(b -> b.getSqlLanguageType() == SqlOperate.dml
        ).name("dml filter"
        ).map(new MapFunction<BinlogBean, BinlogBean>() {
            @Override
            public BinlogBean map(BinlogBean value) throws Exception {
                if (value.getTableName().matches(".*\\d.*")) {
                    // t_std_tree_carmodel_ 分表字段个数不同 在clickhouse里不分表
                    if (!value.getTableName().matches("^t_std_tree_carmodel_[0-9]*$")) {
                        String tabName = value.getTableName();
                        // 把带有数字的表名的数字删掉
                        tabName = tabName.replaceAll("\\d+", "");
                        // 递归删除最后的下划线
                        value.setTableName(lastUnderlineDelete(tabName));
                    }
                }
                return value;
            }
        }).keyBy(BinlogBean::getTSMS
        ).window(TumblingEventTimeWindows.of(Time.seconds(windowMilliseconds))
        ).trigger(EventTimeTrigger.create()
        ).apply(new RichWindowFunction<BinlogBean, Tuple2<Long, Iterable<BinlogBean>>, Long, TimeWindow>() {
            @Override
            public void apply(Long l, TimeWindow w, Iterable<BinlogBean> in
                    , Collector<Tuple2<Long, Iterable<BinlogBean>>> out) throws Exception {
                log.info("聚合的watermark：{}", l);
                out.collect(Tuple2.of(l, in));
            }
        }).name("TSMS aggregation"
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<Long, Iterable<BinlogBean>>>forMonotonousTimestamps(
                ).withTimestampAssigner((SerializableTimestampAssigner<Tuple2<Long, Iterable<BinlogBean>>>)
                        (element, recordTimestamp) -> element.f0)
        ).name("TSMS aggregation watermark");
    }
    private String lastUnderlineDelete(String tableName) {
        if (tableName.length() > 2){
            String underline = tableName.substring(tableName.length()-1);
            if (underline.equals("_")) {
                return lastUnderlineDelete(tableName.substring(0,tableName.length()-1));
            }
        }
        return tableName;
    }
    /**
     * doris异步io执行sql
     * @param source
     * @return binlog数据时间戳,sql执行结果
     */
    public DataStream<Long> sinkDorisOds(
            DataStream<Tuple2<Long, Iterable<BinlogBean>>> source) {

        return AsyncDataStream.orderedWait(source
                , new JdbcAsyncFormat<Tuple2<Long, Iterable<BinlogBean>>
                        , Long>(JdbcType.doris) {
                    @Override
                    public Long asyncInvokeHandle(
                            Tuple2<Long, Iterable<BinlogBean>> t2
                            , JdbcStatement db) throws Exception {
                        int[] exeSqlResult = db.runBatchUpdate(t2.f1, f -> dorisDml(f));
                        log.info("doris 执行了{}条SQL", exeSqlResult.length);
                        return t2.f0;
                    }
                }
                , asyncTimeout, TimeUnit.MINUTES, asyncOpMaxNum
        ).name("doris ods sync"
        ).keyBy(l -> l);
    }
    /**
     * clickhouse异步io执行sql
     * @param source
     * @return binlog数据时间戳,sql执行结果,binlog原始数据
     */
    public DataStream<Tuple2<Long, Iterable<BinlogBean>>> sinkChOds(
            DataStream<Tuple2<Long, Iterable<BinlogBean>>> source) {

        return AsyncDataStream.orderedWait(source
                , new JdbcAsyncFormat<Tuple2<Long, Iterable<BinlogBean>>
                        , Tuple2<Long, Iterable<BinlogBean>>>(JdbcType.clickhouse) {
                    @Override
                    public Tuple2<Long, Iterable<BinlogBean>> asyncInvokeHandle(
                            Tuple2<Long, Iterable<BinlogBean>> t2
                            , JdbcStatement db) throws Exception {
                        int[] exeSqlResult = db.runBatchUpdate(t2.f1, f -> chDml(f));
                        log.info("clickhouse 执行了{}条SQL", exeSqlResult.length);
                        return t2;
                    }
                }
                , asyncTimeout, TimeUnit.MINUTES, asyncOpMaxNum
        ).name("ch ods sync"
        ).keyBy(t2 -> t2.f0);
    }

    public void sinkDorisOrCh(DataStream<Tuple2<Long, Iterable<BinlogBean>>> source, JdbcType sourceType) {
        sink(
        ).jdbcSink(sourceType
                , source.flatMap(new FlatMapFunction<Tuple2<Long, Iterable<BinlogBean>>, BinlogBean>() {
                    @Override
                    public void flatMap(Tuple2<Long, Iterable<BinlogBean>> v, Collector<BinlogBean> out) throws Exception {
                        v.f1.forEach(out::collect);
                    }
                }).name(sourceType.name() + "binlog source persistence")
                , "insert into v1_scps_basics.ods_binlog_source values(?,?,?,?,?,?,?,?,?,?)"
                , (JdbcStatementBuilder<BinlogBean>) (ps, b) -> {
                    ps.setLong(1, b.getTSMS());
                    ps.setInt(2, b.getSqlLanguageType().getType());
                    ps.setInt(3, b.getOperationType().getType());
                    ps.setString(4, b.getDatabase());
                    ps.setString(5, b.getTableName());
                    ps.setString(6, b.getTableKey());
                    ps.setString(7, JSONObject.toJSON(b.getDataBefore()).toString());
                    ps.setString(8, JSONObject.toJSON(b.getDataAfter()).toString());
                    ps.setString(9, b.getDdl());
                    ps.setTimestamp(10, new Timestamp(DateTimeUtils.getTimeStamp()));
                });
    }

    /**
     * clickhouse
     * @param b binlog数据源
     * @return 插入数据sql,binlog原始数据
     */
    private String chDml(BinlogBean b) {
        String tableName = b.getTableName();

        Map<String,Object> addDataMap;
        String data_flag;

        switch (b.getOperationType()) {
            case delete:
                data_flag = "-1";
                addDataMap = b.getDataBefore();
                break;
            case update:
            case insert:
            default:
                data_flag = "1";
                addDataMap = b.getDataAfter();
                break;
        }

        String addSqlModel = "INSERT INTO scps_mysql_ods.%s (%s) \nSELECT %s";

        StringBuilder resultSBKey = new StringBuilder();
        StringBuilder resultSBValue = new StringBuilder();

        //String ID_KEY = stringObjectMap.get("table_key").toString();

        for(String key : addDataMap.keySet()){
            resultSBKey.append(key).append(",");

            Object value = addDataMap.get(key);
            String valueType = value.getClass().toString();
            if(key.contains("time") && valueType.equals("class java.lang.Long")){
                resultSBValue.append("toDateTime(").append(value.toString()).append("/1000),");
            } else {
                // 字符单引号换成中文字符
                value = value.toString().replace("'","‘");
                // 字符\换成中文字符、
                value = value.toString().replace("\\","、");

                resultSBValue.append("'").append(value).append("',");
            }
        }
        //数据版本字段处理
        resultSBKey.append("data_flag");
        resultSBValue.append(data_flag);
        //数据返回
        return String.format(addSqlModel,tableName,resultSBKey,resultSBValue);
    }

    /**
     * doris
     * @param b binlog数据
     * @return binlog时间戳，插入数据的sql
     */
    private String dorisDml(BinlogBean b) {
        String tableName = b.getTableName();
        Map<String, Object> addDataMap;
        String data_flag;
        switch (b.getOperationType()) {
            case delete:
                data_flag = "-1";
                addDataMap = b.getDataBefore();
                break;
            case update:
            case insert:
            default:
                data_flag = "1";
                addDataMap = b.getDataAfter();
                break;
        }

        String addSqlModel = "INSERT INTO scps_mysql_ods.%s (%s) \nvalues (%s)";

        StringBuilder resultSBKey = new StringBuilder();
        StringBuilder resultSBValue = new StringBuilder();

        //String ID_KEY = stringObjectMap.get("table_key").toString();

        for (String key : addDataMap.keySet()) {
            resultSBKey.append(key).append(",");

            Object value = addDataMap.get(key);
            String valueType = value.getClass().toString();
            if (key.contains("time") && valueType.equals("class java.lang.Long")) {
                resultSBValue.append("'").append(DateTimeUtils.stampToDate(Long.valueOf(value.toString()))).append("',");

            } else {
                // 字符单引号换成中文字符
                value = value.toString().replace("'", "‘");
                // 字符\换成中文字符、
                value = value.toString().replace("\\", "、");

                resultSBValue.append("'").append(value).append("',");
            }
        }
        resultSBKey.append("data_flag");
        resultSBValue.append(data_flag);
        //数据返回
        return String.format(addSqlModel, tableName, resultSBKey, resultSBValue);
    }
}
