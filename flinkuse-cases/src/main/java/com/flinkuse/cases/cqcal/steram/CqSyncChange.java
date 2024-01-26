package com.flinkuse.cases.cqcal.steram;

import com.alibaba.fastjson.JSON;
import com.bestvike.linq.IEnumerable;
import com.bestvike.linq.IGrouping;
import com.bestvike.linq.Linq;
import com.flinkuse.cases.common.CountAndTimeTrigger;
import com.flinkuse.cases.cqcal.constant.CqFinal;
import com.flinkuse.cases.cqcal.entity.CqEsEntity;
import com.flinkuse.cases.cqcal.util.CommonUtil;
import com.flinkuse.core.base.StreamApp;
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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchEmitter;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author learn
 * @date 2023/12/5 10:08
 */
public class CqSyncChange extends StreamApp {

    private final String odsTopicGroup;
    private final String offset;
    private final long windowsMinutes;
    private final long windowsCount;
    private final int capacity;
    private final int sqlIdNumber;
    /**
     * 索引前缀
     */
    private final String indexPrefix;
    /**
     * 索引后缀
     */
    private final String indexSuffix;
    /**
     * 原数据索引表名
     */
    private final String otConf;

    /**
     * 应用车型id对应的tree_id
     */
    private Map<String, String[]> cmTreeId;
    /**
     * oem、std、sku产品属性配置
     */
    private Map<String, Map<String, Object>> proUtil;
    private Map<String, Tuple2<String, String>> tbColumn;

    private final String skuIndex;
    private final String stdIndex;
    private final String oemIndex;
    private final String insIndex;
    private final String fmsIndex;
    private final String skuAdapterIndex;
    private final String carModelIndex;

    public CqSyncChange(String[] args, String jobName) {
        super(args, jobName);
        offset = getScpsParams().get("cq_offset", "latest");
        odsTopicGroup = getScpsParams().get("cq_ods_topic_group", "cq-group");
        windowsMinutes = getScpsParams().getLong("cq_windows_time", 5L);
        windowsCount = getScpsParams().getLong("cq_windows_count", 5000L);
        capacity = getScpsParams().getInt("cq_capacity", 5);
        sqlIdNumber = getScpsParams().getInt("cq_sql_number", 10000);

        indexPrefix = getScpsParams().get("cq_index_prefix", "basics_cq_");
        indexSuffix = getScpsParams().get("cq_index_suffix", "_" + DateTimeUtils.dateNowFormat("yyyyMMddHHmmss"));
        otConf = getScpsParams().get("cq_ot_conf", "t_famous_std_rel");

        skuIndex = indexPrefix + "sku" + indexSuffix;
        stdIndex = indexPrefix + "std" + indexSuffix;
        oemIndex = indexPrefix + "oem" + indexSuffix;
        insIndex = indexPrefix + "ins" + indexSuffix;
        fmsIndex = indexPrefix + "fms" + indexSuffix;
        skuAdapterIndex = indexPrefix + "cmskurel" + indexSuffix;
        carModelIndex = indexPrefix + "cm" + indexSuffix;
    }

    public static void main(String[] args) throws Exception {
        new CqSyncChange(args,"category query stream").start();
    }
    @Override
    public void run(StreamExecutionEnvironment streamEnv) {

        Tuple3<Map<String, String[]>
                , Map<String, Map<String, Object>>
                , Map<String, Tuple2<String, String>>>
                t3 = CommonUtil.dataInit(getScpsConfig(), otConf);
        proUtil = t3.f1;
        cmTreeId = t3.f0;
        tbColumn = t3.f2;
        // 获取数据源 聚合id 横向的6个id
        DataStream<Map<String, Object>> cqSource = sourceLogic();

        // 聚合数据
        DataStream<Map<String, Set<String>>> InsGroupData = InsGroupLogic(cqSource);

        // 查询clickhouse
        DataStream<List<CqEsEntity>> chData = AsyncDataStream
                .orderedWait(InsGroupData, new JdbcAsyncFormat<Map<String, Set<String>>, List<CqEsEntity>>(JdbcConnectionType.clickhouse) {
                    @Override
                    public List<CqEsEntity> asyncInvokeInputHandle(Map<String, Set<String>> i, JdbcStatementFunction j) throws Exception {
                        return chQueryData(i, j);
                    }
                }
                , windowsMinutes
                , TimeUnit.MINUTES
                , capacity
        ).name("ch async");

        // 格式化数据
        DataStream<CqEsEntity> sinkData = insDataFormat(chData).union(delDataFormat(cqSource));

        // 输出到es
        cqSink(sinkData);
    }

    private void originalTableLogic(DataStream<BinlogBean> toTagData) {
        cqSink(toTagData.map(new MapFunction<BinlogBean, CqEsEntity>() {
            @Override
            public CqEsEntity map(BinlogBean e) throws Exception {
                Map<String,Object> data;
                CqEsEntity res = new CqEsEntity();
                res.setIndex(indexPrefix + e.getTableName() + indexSuffix);
                switch (e.getOperationType()) {
                    case delete:
                        data = e.getDataBefore();

                        res.setAction(SqlOperate.delete);
                        res.set_id(data.get(e.getTableKey()).toString());
                        return res;
                    case insert:
                    case update:
                    default:
                        data = e.getDataAfter();
                        data.replaceAll((k, v) -> Objects.toString(v, ""));

                        res.setAction(SqlOperate.insert);
                        res.set_id(data.get(e.getTableKey()).toString());
                        res.setData(data);
                        return res;
                }
            }
        }));
    }

    private void cqSink(DataStream<CqEsEntity> sinkData) {
        sink().elasticsearchSink(sinkData, (ElasticsearchEmitter<CqEsEntity>) (e, c, i) -> {
            switch (e.getAction()) {
                case delete:
                    i.add(Requests.deleteRequest(e.getIndex()).id(e.get_id()));
                    break;
                /* 批量修改
                case update:
                    Map<String,Object> data = (Map<String,Object>) e.getData();
                    List<String> sss = (List<String>) data.get("_ids");
                    Map<String, Object> updateData = (Map<String, Object>) data.get("update_data");
                    for (String s : sss) {
                        UpdateRequest request = new UpdateRequest(e.getIndex(), s);
                        XContentBuilder scb =  XContentFactory.jsonBuilder().startObject();
                        for (Map.Entry<String, Object> entry : updateData.entrySet()) {
                            scb.field(entry.getKey(), entry.getValue());
                        }
                        request.doc(scb.endObject());
                        i.add(request);
                    }
                    break;
                 */
                case insert:
                    Map<String, Object> d = (Map<String,Object>) e.getData();
                    d.put("watermark", c.currentWatermark());

                    i.add(Requests.indexRequest(
                    ).index(e.getIndex()
                    ).source(d, XContentType.JSON
                    ).id(e.get_id()));
                    break;
            }
        });
    }

    private DataStream<CqEsEntity> insDataFormat(DataStream<List<CqEsEntity>> d) {
        return d.flatMap(new FlatMapFunction<List<CqEsEntity>, CqEsEntity>() {
            @Override
            public void flatMap(List<CqEsEntity> value, Collector<CqEsEntity> out) throws Exception {
                value.forEach(out::collect);
            }
        }).flatMap(new FlatMapFunction<CqEsEntity, CqEsEntity>() {
            @Override
            public void flatMap(CqEsEntity v, Collector<CqEsEntity> out) throws Exception {
                if (v.getAction() == SqlOperate.insert) {
                    List<Map<String, Object>> td = (List<Map<String, Object>>) v.getData();
                    for (Map<String, Object> res : td) {
                        CqEsEntity c = new CqEsEntity();
                        c.setAction(v.getAction());
                        c.setIndex(v.getIndex());
                        c.setWatermark(v.getWatermark());

                        if (c.getIndex().equals(oemIndex)) {
                            c.set_id(res.get("oem_partsku_id").toString());
                            res.put("oem_partsku_codes", CommonUtil.jsonAnalysis(res.get("oem_partsku_code").toString(), log));
                            res.remove("oem_partsku_code");
                        }
                        if (c.getIndex().equals(fmsIndex)) {
                            c.set_id(res.get("fms_partsku_id").toString());
                            res.put("fms_partsku_codes", CommonUtil.jsonAnalysis(res.get("fms_partsku_code").toString(), log));
                            res.remove("fms_partsku_code");
                        }
                        if (c.getIndex().equals(insIndex)) {
                            c.set_id(res.get("indus_partsku_id").toString());
                            res.put("indus_partsku_codes", CommonUtil.jsonAnalysis(res.get("indus_partsku_code").toString(), log));
                            res.remove("indus_partsku_code");
                        }
                        if (c.getIndex().equals(stdIndex)) {
                            c.set_id(res.get("std_partsku_id").toString());
                        }
                        if (c.getIndex().equals(skuIndex)) {
                            c.set_id(res.get("ten_partsku_id").toString());
                            res.put("partsku_exchange_codes", CommonUtil.jsonAnalysis(res.get("partsku_exchange_code").toString(), log));
                            res.remove("partsku_exchange_code");
                        }
                        if (c.getIndex().equals(skuAdapterIndex)) {
                            res.replaceAll((k, vv) -> Objects.toString(vv, ""));
                            String val = Objects.toString(res.get("remark_extend"));
                            if (!(val.equals("") || val.equals("null"))) {
                                res.put("remark_extend", JSON.parseObject(val, List.class));
                            } else {
                                res.remove("remark_extend");
                            }
                            c.set_id(res.get("ten_cm_rel_id").toString());
                        }

                        c.setData(res);
                        out.collect(c);
                    }
                }
            }
        });
    }

    private List<Map<String, Object>> skuValue(String[] valueArray, String name) {
        List<Map<String, Object>> resultSkuVal = new ArrayList<>();
        if (valueArray.length > 0) {
            for (String skuVal : valueArray) {
                String[] skuKV = skuVal.split(":");
                Map<String, Object> skuValueMsg = proUtil.get(skuKV[0]);
                if (skuValueMsg != null) {
                    if (skuKV.length > 1)
                        skuValueMsg.put(name, skuKV[1]);
                    else
                        skuValueMsg.put(name, "");
                    resultSkuVal.add(skuValueMsg);
                }
            }
        }
        return resultSkuVal;
    }

    private List<Map<String,Object>> association(List<Map<String,Object>> l
            , List<Map<String,Object>> r
            , String key) {

        String imgName = "images";
        String valName = "values";
        switch (key) {
            case "oem_partsku_id":
                imgName = "oem_partsku_images";
                valName = "oem_partsku_value";
                break;
            case "fms_partsku_id":
                imgName = "fms_partsku_images";
                valName = "fms_partsku_value";
                break;
            case "indus_partsku_id":
                imgName = "indus_partsku_images";
                valName = "indus_partsku_value";
                break;
            case "std_partsku_id":
                imgName = "std_partsku_images";
                valName = "std_partsku_value";
                break;
            case "ten_partsku_id":
                imgName = "ten_partsku_images";
                valName = "ten_partsku_value";
                break;
            // case "std_tree_cm_id":
        }

        Map<String,List<Map<String,Object>>> imgGroup = Linq.of(r).groupBy(g -> g.get(key).toString()
        ).toMap(IGrouping::getKey, IEnumerable::toList);

        String finalImgName = imgName;
        String finalValName = valName;
        return Linq.of(l).leftJoin(Linq.of(imgGroup)
                , jr -> jr.get(key).toString()
                , Map.Entry::getKey
                , (jr, jl) -> {
                    jr.replaceAll((k, v) -> Objects.toString(v, ""));
                    if (jl != null && jl.getValue() != null && jl.getValue().size() > 0) {
                        List<Map<String,Object>> imgs = jl.getValue();
                        for (Map<String,Object> mso : imgs) {
                            mso.replaceAll((k, v) -> Objects.toString(v, ""));
                            mso.remove(key);
                        }
                        jr.put(finalImgName, imgs);
                    }
                    else
                        jr.put(finalImgName, new ArrayList<>());

                    jr.put(finalValName + "s", skuValue(Objects.toString(jr.get(finalValName),"").split(","), finalValName));
                    jr.remove(finalValName);

                    return jr;
                }
        ).toList();
    }

    private List<String> generate(String key, Set<String> value) {
        List<String> res = new ArrayList<>();
        String[] original = value.toArray(new String[0]);
        // 分片操作
        for (int i = 0; i < original.length; i += sqlIdNumber) {
            // 计算每个子集的起始和结束索引
            int end = Math.min(i + sqlIdNumber, original.length);
            // 创建一个新的子集
            StringBuilder sb = new StringBuilder();
            sb.append(" ").append(key).append(" GLOBAL IN (");
            // 将范围内的元素添加到子集中
            for (int j = i; j < end; j++) {
                sb.append(original[j]).append(",");
            }

            sb.delete(sb.length() - 1, sb.length());
            sb.append(") AND ");
            log.info(" IDS \n" + sb + "\n");
            // 将子集添加到分片数组中
            res.add(sb.toString());
        }

        return res;
    }

    private List<CqEsEntity> chQueryData(Map<String, Set<String>> i, JdbcStatementFunction j) throws SQLException {
        List<CqEsEntity> res = new ArrayList<>();

        Set<String> oemId = i.get("oem_partsku_id");
        Set<String> stdId = i.get("std_partsku_id");
        Set<String> tenId = i.get("ten_partsku_id");
        Set<String> insId = i.get("indus_partsku_id");
        Set<String> fmsId = i.get("fms_partsku_id");
        Set<String> cmTenId = i.get("ten_cm_rel_id");
        Set<String> yyCmId = i.get("std_tree_cm_rel_id");

        if (yyCmId.size() > 0) {
            // 以属性配置id分组
            Map<String, List<String>> jj = new HashMap<>();
            for (String tid : yyCmId) {
                // 拆分 0：std_tree_cm_id 1：std_tree_cm_conf_id
                String[] tids = tid.split(",");
                List<String> ls;
                if (!jj.containsKey(tids[1])) {
                    ls = new ArrayList<>();
                } else {
                    ls = jj.get(tids[1]);
                }
                ls.add(tids[0]);
                jj.put(tids[1], ls);
            }
            List<Map<String,Object>> dat = new ArrayList<>();
            // 循环属性配置id
            for (Map.Entry<String, List<String>> entry : jj.entrySet()) {

                for (String cmIdSql : generate("std_tree_cm_id", new HashSet<>(entry.getValue()))) {

                    String cmProSql = "select\n\ta.*,b.cm_id\nFROM\n\t(\n\tselect\n\t\t*\n\tFROM\n\t\tscps_mysql_ods.t_std_tree_carmodel_${std_tree_cm_conf_id} final\n\twhere ${std_tree_cm_id}\n\t\tdata_flag > 0) a\nJOIN (\n\tselect\n\t\tcm_id,\n\t\tstd_tree_cm_id\n\tfrom\n\t\tscps_mysql_ods.t_std_tree_carmodel_rel final\n\twhere ${std_tree_cm_id}\n\t\tdata_flag > 0) b ON a.std_tree_cm_id = b.std_tree_cm_id";
                    cmProSql = cmProSql.replace("${std_tree_cm_conf_id}", entry.getKey()).replace("${std_tree_cm_id}", cmIdSql);

                    // log.info("\n" + cmProSql + "\n");
                    List<Map<String, Object>> cmIdProId = j.runQuery(cmProSql);
                    // 构造配件应用id map
                    Map<String, Object> sti = new HashMap<>();
                    for (String stdTreeId : cmTreeId.get(entry.getKey())) {
                        sti.put("std_tree_id_" + stdTreeId, 1);
                    }
                    // 查询出来的车型信息 添加配件应用id
                    cmIdProId.forEach(f -> {
                        f.putAll(sti);
                        f.remove("data_flag");
                        f.remove("create_time");
                        f.remove("create_by");
                        f.replaceAll((k, v) -> v.toString());
                    });
                    // 添加到结果列表
                    dat.addAll(cmIdProId);
                }
                if (dat.size() > 0) {
                    CqEsEntity a = new CqEsEntity();
                    a.setAction(SqlOperate.insert);
                    a.setData(dat);
                    a.setIndex(carModelIndex);

                    res.add(a);
                }
            }
        }

        if (fmsId.size() > 0) {
            for (String fmsIdSql : generate("fms_partsku_id", fmsId)) {
                String fmsMsgSql = CqFinal.fmsMsg.replace("${fms_partsku_id}", fmsIdSql);
                String fmsImgSql = CqFinal.fmsImg.replace("${fms_partsku_id}", fmsIdSql);
                List<Map<String,Object>> dat = association(j.runQuery(fmsMsgSql)
                        , j.runQuery(fmsImgSql)
                        , "fms_partsku_id");
                if (dat.size() > 0) {
                    CqEsEntity a = new CqEsEntity();
                    a.setAction(SqlOperate.insert);
                    a.setData(dat);
                    a.setIndex(fmsIndex);

                    res.add(a);
                }
            }
        }
        if (insId.size() > 0) {
            for (String insIdSql : generate("indus_partsku_id", insId)) {
                List<Map<String, Object>> dat = association(j.runQuery(CqFinal.insMsg.replace("${indus_partsku_id}", insIdSql))
                        , j.runQuery(CqFinal.insImg.replace("${indus_partsku_id}", insIdSql))
                        , "indus_partsku_id");
                if (dat.size() > 0) {
                    CqEsEntity a = new CqEsEntity();
                    a.setAction(SqlOperate.insert);
                    a.setData(dat);
                    a.setIndex(insIndex);

                    res.add(a);
                }
            }
        }
        if (oemId.size() > 0) {
            for (String oemIdSql : generate("oem_partsku_id", oemId)) {
                List<Map<String, Object>> dat = association(j.runQuery(CqFinal.oemMsg.replace("${oem_partsku_id}", oemIdSql))
                        , j.runQuery(CqFinal.oemImg.replace("${oem_partsku_id}", oemIdSql))
                        , "oem_partsku_id");
                if (dat.size() > 0) {
                    CqEsEntity a = new CqEsEntity();
                    a.setAction(SqlOperate.insert);
                    a.setData(dat);
                    a.setIndex(oemIndex);

                    res.add(a);
                }
            }
        }
        if (stdId.size() > 0) {
            for (String stdIdSql : generate("std_partsku_id", stdId)) {
                List<Map<String, Object>> dat = association(j.runQuery(CqFinal.stdMsg.replace("${std_partsku_id}", stdIdSql))
                        , j.runQuery(CqFinal.stdImg.replace("${std_partsku_id}", stdIdSql))
                        , "std_partsku_id");
                if (dat.size() > 0) {
                    CqEsEntity a = new CqEsEntity();
                    a.setAction(SqlOperate.insert);
                    a.setData(dat);
                    a.setIndex(stdIndex);
                    res.add(a);
                }
            }
        }
        if (tenId.size() > 0) {
            for (String tenIdSql : generate("ten_partsku_id", tenId)) {
                List<Map<String, Object>> dat = association(j.runQuery(CqFinal.skuMsg.replace("${ten_partsku_id}", tenIdSql))
                        , j.runQuery(CqFinal.skuImg.replace("${ten_partsku_id}", tenIdSql))
                        , "ten_partsku_id");
                if (dat.size() > 0) {
                    CqEsEntity a = new CqEsEntity();
                    a.setAction(SqlOperate.insert);
                    a.setData(dat);
                    a.setIndex(skuIndex);

                    res.add(a);
                }
            }
        }
        if (cmTenId.size() > 0) {

            for (String relIdSql : generate("ten_cm_rel_id", cmTenId)) {
                String sqldd = CqFinal.skuAdapter.replace("${ten_cm_rel_id}", relIdSql);
                List<Map<String, Object>> relData = j.runQuery(sqldd);

                if (relData.size() > 0) {
                    CqEsEntity a = new CqEsEntity();
                    a.setAction(SqlOperate.insert);
                    a.setData(relData);
                    a.setIndex(skuAdapterIndex);

                    res.add(a);
                }
            }
        }

        return res;
    }

    private DataStream<CqEsEntity> delDataFormat(DataStream<Map<String, Object>> source) {
        return source.filter((FilterFunction<Map<String, Object>>) value -> value.get("operation_type").toString().equals(SqlOperate.delete.getName())
        ).map(new MapFunction<Map<String, Object>, CqEsEntity>() {
            @Override
            public CqEsEntity map(Map<String, Object> i) throws Exception {
                CqEsEntity res = new CqEsEntity();
                res.setAction(SqlOperate.delete);
                if (i.containsKey("oem_partsku_id")) {
                    res.setIndex(oemIndex);
                    res.set_id(i.get("oem_partsku_id").toString());
                }
                if (i.containsKey("std_partsku_id")) {
                    res.setIndex(stdIndex);
                    res.set_id(i.get("std_partsku_id").toString());
                }
                if (i.containsKey("ten_partsku_id")) {
                    res.setIndex(skuIndex);
                    res.set_id(i.get("ten_partsku_id").toString());
                }
                if (i.containsKey("indus_partsku_id")) {
                    res.setIndex(insIndex);
                    res.set_id(i.get("indus_partsku_id").toString());
                }
                if (i.containsKey("fms_partsku_id")) {
                    res.setIndex(fmsIndex);
                    res.set_id(i.get("fms_partsku_id").toString());
                }
                if (i.containsKey("ten_cm_rel_id")) {
                    res.setIndex(skuAdapterIndex);
                    res.set_id(i.get("ten_cm_rel_id").toString());
                }
                return res;
            }
        });
    }

    private DataStream<Map<String, Set<String>>> InsGroupLogic(DataStream<Map<String, Object>> source) {
        return source.filter(
                (FilterFunction<Map<String, Object>>) value -> value.get("operation_type").toString().equals(SqlOperate.insert.getName())
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Map<String,Object>>forMonotonousTimestamps(
                ).withTimestampAssigner((SerializableTimestampAssigner<Map<String,Object>>) (ob, l) -> Long.parseLong(ob.get("ts_ms").toString()))
        ).name("wm order"
        ).windowAll(GlobalWindows.create()
        ).trigger(new CountAndTimeTrigger<>(windowsCount, Time.minutes(windowsMinutes))
        ).process(new ProcessAllWindowFunction<Map<String, Object>, Map<String, Set<String>>, GlobalWindow>() {
            @Override
            public void process(Context c, Iterable<Map<String, Object>> e, Collector<Map<String, Set<String>>> o) throws Exception {
                Set<String> oemId = new HashSet<>();
                Set<String> stdId = new HashSet<>();
                Set<String> tenId = new HashSet<>();
                Set<String> insId = new HashSet<>();
                Set<String> fmsId = new HashSet<>();
                Set<String> cmTenId = new HashSet<>();
                Set<String> yyCmId = new HashSet<>();

                for (Map<String, Object> map : e) {
                    if (map.containsKey("oem_partsku_id")) {
                        oemId.add(map.get("oem_partsku_id").toString());
                    }
                    if (map.containsKey("fms_partsku_id")) {
                        fmsId.add(map.get("fms_partsku_id").toString());
                    }
                    if (map.containsKey("indus_partsku_id")) {
                        insId.add(map.get("indus_partsku_id").toString());
                    }
                    if (map.containsKey("std_partsku_id")) {
                        stdId.add(map.get("std_partsku_id").toString());
                    }
                    if (map.containsKey("ten_partsku_id")) {
                        tenId.add(map.get("ten_partsku_id").toString());
                    }
                    if (map.containsKey("ten_cm_rel_id")) {
                        cmTenId.add(map.get("ten_cm_rel_id").toString());
                    }
                    if (map.containsKey("std_tree_cm_rel_id")) {
                        yyCmId.add(map.get("std_tree_cm_rel_id").toString());
                    }
                }
                Map<String, Set<String>> result = new HashMap<>();
                result.put("oem_partsku_id", oemId);
                result.put("fms_partsku_id", fmsId);
                result.put("indus_partsku_id", insId);
                result.put("std_partsku_id", stdId);
                result.put("ten_partsku_id", tenId);
                result.put("ten_cm_rel_id", cmTenId);
                result.put("std_tree_cm_rel_id", yyCmId);
                o.collect(result);
            }
        }).name("line map id");
    }

    private DataStream<Map<String, Object>> sourceLogic() {
        OutputTag<BinlogBean> otTag = new OutputTag<>("original_table") {};

        SingleOutputStreamOperator<Map<String, Object>> source = source(
        ).kafkaSource(KafkaSource.<BinlogBean>builder(
                ).setBootstrapServers(getScpsConfig().get(ConfigKeys.kafka_bootstrap_servers)
                ).setTopics(OdsBasicsConf.odsTopic
                ).setGroupId(odsTopicGroup
                ).setStartingOffsets(StringUtils.toKafkaOffset(offset)
                ).setValueOnlyDeserializer(new KafkaDeserializerBinlog()
                ).build()
                , WatermarkStrategy.noWatermarks()
                // 过滤掉不需要的表数据
        ).process(new ProcessFunction<BinlogBean, Map<String, Object>>() {
            @Override
            public void processElement(BinlogBean value, Context ctx, Collector<Map<String, Object>> out) throws Exception {

                String tbName = value.getTableName();
                /* 在clickhouse里t_std_tree_carmodel_表由于字段不同 分表
                if (tbName.matches("^t_std_tree_carmodel_[0-9]*$"))
                    tbName = "submitters_no";
                 */
                /*
                 侧输出原表
                 */
                if (tbColumn.containsKey(tbName))
                    ctx.output(otTag, value);

                Map<String,Object> data;
                Map<String,Object> result = new HashMap<>();
                result.put("ts_ms", value.getTSMS());

                switch (value.getOperationType()) {
                    case delete:
                        data = value.getDataBefore();
                        // 表为主表并且类型为删除
                        if (tbName.equals("t_oem_partsku")
                                || tbName.equals("t_famous_partsku")
                                || tbName.equals("t_industry_partsku")
                                || tbName.equals("t_standard_partsku")
                                || tbName.equals("t_tenant_partsku")
                                || tbName.equals("t_tenant_cm_rel")
                                || tbName.equals("t_std_tree_carmodel_rel")
                        )
                            result.put("operation_type", SqlOperate.delete.getName());
                        else
                            result.put("operation_type", SqlOperate.insert.getName());
                        break;
                    case insert:
                    case update:
                    default:
                        data = value.getDataAfter();
                        result.put("operation_type", SqlOperate.insert.getName());
                        break;
                }

                switch (tbName) {
                    case "t_oem_partsku_image_rel":
                    case "t_oem_partsku":
                    case "t_oem_partsku_value":
                        result.put("oem_partsku_id", data.get("oem_partsku_id"));
                        break;

                    case "t_famous_partsku_image_rel":
                    case "t_famous_partsku":
                    case "t_famous_partsku_value":
                        result.put("fms_partsku_id", data.get("fms_partsku_id"));
                        break;

                    case "t_industry_partsku_image_rel":
                    case "t_industry_partsku":
                    case "t_industry_partsku_value":
                        result.put("indus_partsku_id", data.get("indus_partsku_id"));
                        break;

                    case "t_standard_partsku_image_rel":
                    case "t_standard_partsku":
                    case "t_standard_partsku_value":
                        result.put("std_partsku_id", data.get("std_partsku_id"));
                        break;

                    case "t_tenant_partsku_image_rel":
                    case "t_tenant_partsku":
                    case "t_tenant_partsku_value":
                    case "t_tenant_partsku_exchange":
                        result.put("ten_partsku_id", data.get("ten_partsku_id"));
                        break;

                    case "t_tenant_cm_rel":
                        result.put("ten_cm_rel_id", data.get("ten_cm_rel_id"));
                        break;

                    /* 车型特殊 需要std_tree_cm_id和std_tree_cm_conf_id  std_tree_cm_rel_id t_std_tree_carmodel_rel
                    case "submitters_no": */
                    case "t_std_tree_carmodel_rel":
                        result.put("std_tree_cm_rel_id", data.get("std_tree_cm_rel_id").toString() + "," + data.get("std_tree_cm_conf_id"));
                        break;
                }
                /*
                result有三个元素
                1.ts_ms
                2.operation_type
                3.每个索引的主键ID
                 */
                if (result.size() == 3)
                    out.collect(result);
            }
        }).name("kf tb filter");

        /*
         原表索引处理
         */
        originalTableLogic(source.getSideOutput(otTag));

        return source;
    }
}
