package com.flinkuse.cases.cqcal.batch;

import com.alibaba.fastjson.JSON;
import com.flinkuse.cases.cqcal.constant.CqFinal;
import com.flinkuse.cases.cqcal.util.CommonUtil;
import com.flinkuse.core.base.BatchApp;
import com.flinkuse.core.enums.JdbcConnectionType;
import com.flinkuse.core.util.DateTimeUtils;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * @author learn
 * @date 2023/9/8 10:07
 */
public class CqSyncInit extends BatchApp {
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

    private final Integer control;
    /**
     * oem、std、sku产品属性配置
     */
    private Map<String, Map<String, Object>> proUtil;
    /**
     * 应用车型id对应的tree_id
     */
    private Map<String, String[]> cmTreeId;
    private Map<String, Tuple2<String, String>> tbColumn;

    public CqSyncInit(String[] args, String jobName) {
        super(args, jobName);

        indexPrefix = getScpsParams().get("cq_index_prefix", "basics_cq_");
        indexSuffix = getScpsParams().get("cq_index_suffix", "_" + DateTimeUtils.dateNowFormat("yyyyMMddHHmmss"));
        otConf = getScpsParams().get("cq_ot_conf", "t_famous_std_rel");

        control = getScpsParams().getInt("cq_control", 0);
    }

    public static void main(String[] args) throws Exception {
        new CqSyncInit(args, "category query batch").start();
    }

    @Override
    public void run(ExecutionEnvironment batchEnv) {
        try {

            Tuple3<Map<String, String[]>
                    , Map<String, Map<String, Object>>
                    , Map<String, Tuple2<String, String>>>
                    t3 = CommonUtil.dataInit(getScpsConfig(), otConf);
            proUtil = t3.f1;
            cmTreeId = t3.f0;
            tbColumn = t3.f2;

            switch (control) {
                case 0:
                    skuTrans();// 产品
                    skuAdapterTrans();// 产品适配
                    stdTrans();// 标准码信息
                    insTrans();// 行业码信息
                    fmsTrans();// 大厂码信息
                    oemTrans();// oem码信息
                    carModelTrans();// 应用车型
                    originalTable();// 原表
                    break;
                case 1:
                    skuTrans();// 产品
                    break;
                case 2:
                    skuAdapterTrans();// 产品适配
                    break;
                case 3:
                    stdTrans();// 标准码信息
                    break;
                case 4:
                    insTrans();// 行业码信息
                    break;
                case 5:
                    fmsTrans();// 大厂码信息
                    break;
                case 6:
                    oemTrans();// oem码信息
                    break;
                case 7:
                    carModelTrans();// 应用车型
                    break;
                case 8:
                    originalTable();// 原表
                    break;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void originalTable() throws Exception {
        for (Map.Entry<String, Tuple2<String, String>> en : tbColumn.entrySet()) {
            String sql = "select " + en.getValue().f1 + " from scps_mysql_ods." + en.getKey() + " final where data_flag > 0";

            String tableId = en.getValue().f0;

            sink().elasticsearchSink(source().jdbcSource(JdbcConnectionType.clickhouse, sql
            ).flatMap(new FlatMapFunction<Row, Map<String, Object>>() {
                @Override
                public void flatMap(Row value, Collector<Map<String, Object>> out) throws Exception {
                    Map<String, Object> result = new HashMap<>();
                    for (String col : Objects.requireNonNull(value.getFieldNames(true))) {
                        String val = Objects.toString(value.getField(col));
                        result.put(col, val);
                    }
                    result.put("watermark", DateTimeUtils.getTimeStamp());
                    result.put("_id", result.get(tableId).toString());
                    out.collect(result);
                }
            }), indexPrefix + en.getKey() + indexSuffix);
        }
    }

    /**
     * 应用车型索引
     * 关联cm_id
     * @throws Exception
     */
    private void carModelTrans() throws Exception {

        DataSet<Map<String, Object>> ss = source().jdbcSource(JdbcConnectionType.clickhouse
                , "select std_tree_cm_id,std_tree_cm_conf_id,data_json from v1_scps_basics.ods_cq_std_tree_carmodel"
        ).map(new MapFunction<Row, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(Row value) throws Exception {
                Map<String, Object> result = new HashMap<>();
                result.put("std_tree_cm_id", Objects.toString(value.getField("std_tree_cm_id")));
                String cConfId = Objects.toString(value.getField("std_tree_cm_conf_id"));
                result.put("std_tree_cm_conf_id", cConfId);
                if (cmTreeId.containsKey(cConfId)) {
                    for (String id : cmTreeId.get(cConfId)) {
                        result.put("std_tree_id_" + id, 1);
                    }
                }
                result.putAll(JSON.parseObject(Objects.toString(value.getField("data_json")), Map.class));

                return result;
            }
        });

        DataSet<Map<String, Object>> esResult = source().jdbcSource(JdbcConnectionType.clickhouse
                , "select std_tree_cm_rel_id,cm_id,std_tree_cm_id from scps_mysql_ods.t_std_tree_carmodel_rel final where data_flag > 0"
        ).join(ss
        ).where(l -> Objects.toString(l.getField("std_tree_cm_id"))
        ).equalTo(r -> r.get("std_tree_cm_id").toString()
        ).with(new FlatJoinFunction<Row, Map<String, Object>, Map<String, Object>>() {
                   @Override
                   public void join(Row first, Map<String, Object> second, Collector<Map<String, Object>> out) throws Exception {
                       second.put("cm_id", Objects.toString(first.getField("cm_id")));
                       second.put("std_tree_cm_rel_id", Objects.toString(first.getField("std_tree_cm_rel_id")));
                       second.put("_id", Objects.toString(first.getField("std_tree_cm_rel_id")));

                       out.collect(second);
                   }
               }
//        ).groupBy(m -> m.get("std_tree_cm_conf_id").toString()
//        ).reduceGroup(new GroupReduceFunction<Map<String, Object>, Map<String, Object>>() {
//            @Override
//            public void reduce(Iterable<Map<String, Object>> v, Collector<Map<String, Object>> o) throws Exception {
//                List<String> cms = new ArrayList<>();
//                Map<String, Object> result = new HashMap<>();
//                for (Map<String, Object> mm : v) {
//                    result = mm;
//                    cms.add(mm.get("cm_id").toString());
//                }
//                result.remove("cm_id");
//                result.put("cm_ids", cms);
//                o.collect(result);
//            }
//        }
        );

        sink().elasticsearchSink(esResult, indexPrefix + "cm" + indexSuffix);
    }

    /**
     * cm——sku关系索引
     * sql获取cm-oem备注 车型属性备注sql拼接成json
     * @throws Exception
     */
    private void skuAdapterTrans() throws Exception {
        DataSet<Map<String, Object>> skuSet = source().jdbcSource(JdbcConnectionType.clickhouse
                , CqFinal.skuAdapter.replace("${ten_cm_rel_id}", "")
        ).flatMap(new FlatMapFunction<Row, Map<String, Object>>() {
            @Override
            public void flatMap(Row value, Collector<Map<String, Object>> out) throws Exception {
                Map<String, Object> result = new HashMap<>();
                for (String col : Objects.requireNonNull(value.getFieldNames(true))) {
                    String val = Objects.toString(value.getField(col));
                    if (!(val.equals("") || val.equals("null"))){
                        if (col.equals("remark_extend")) {
                            result.put(col, JSON.parseObject(val, List.class));
                        } else {
                            result.put(col, val);
                        }
                    }
                }
                result.put("watermark", DateTimeUtils.getTimeStamp());

                result.put("_id", result.get("ten_cm_rel_id"));
                out.collect(result);
            }
        });
        sink().elasticsearchSink(skuSet, indexPrefix + "cmskurel" + indexSuffix);
    }

    /**
     * 产品信息索引
     * sql获取属性
     * 算子关联图片信息
     * @throws Exception
     */
    private void skuTrans() throws Exception {
        DataSet<Map<String, Object>> skuSet = source().jdbcSource(JdbcConnectionType.clickhouse,
                CqFinal.skuMsg.replace("${ten_partsku_id}","")
        ).flatMap(new FlatMapFunction<Row, Map<String, Object>>() {
            @Override
            public void flatMap(Row value, Collector<Map<String, Object>> out) throws Exception {
                Map<String, Object> result = new HashMap<>();
                String[] skuValueArray = Objects.toString(value.getFieldAs("ten_partsku_value"),"").split(",");
                List<Map<String, Object>> resultSkuVal = new ArrayList<>();
                if (skuValueArray.length > 0) {
                    for (String skuVal : skuValueArray) {
                        String[] skuKV = skuVal.split(":");
                        Map<String, Object> skuValueMsg = proUtil.get(skuKV[0]);
                        if (skuValueMsg != null) {
                            if (skuKV.length > 1)
                                skuValueMsg.put("ten_partsku_value", skuKV[1]);
                            else
                                skuValueMsg.put("ten_partsku_value", "");
                            resultSkuVal.add(skuValueMsg);
                        }
                    }
                    // resultSkuVal.sort(Comparator.comparing(t -> t.get("category_pro_index").toString()));
                }
                result.put("ten_partsku_values", resultSkuVal);

                result.put("partsku_exchange_codes", CommonUtil.jsonAnalysis(value.getFieldAs("partsku_exchange_code"), log));

                for (String col : Objects.requireNonNull(value.getFieldNames(true))) {
                    if (!col.equals("ten_partsku_value") && !col.equals("partsku_exchange_code"))
                        result.put(col, Objects.toString(value.getField(col)));
                }
                out.collect(result);
            }
        }).name("sku source map");

        DataSet<Map<String, Object>> skuImgSet = source().jdbcSource(JdbcConnectionType.clickhouse,
                CqFinal.skuImg.replace("${ten_partsku_id}","")
        ).groupBy(r -> Objects.toString(r.getField("ten_partsku_id"))
        ).reduceGroup(new GroupReduceFunction<Row, Map<String, Object>>() {
            @Override
            public void reduce(Iterable<Row> values, Collector<Map<String, Object>> out) throws Exception {
                Map<String, Object> result = new HashMap<>();
                List<Map<String, Object>> data = new ArrayList<>();

                values.forEach(r -> {
                    Map<String, Object> val = new HashMap<>();
                    for (String cloName : Objects.requireNonNull(r.getFieldNames(false))) {
                        val.put(cloName, Objects.toString(r.getField(cloName)));
                    }
                    val.remove("ten_partsku_id");
                    data.add(val);
                    result.put("ten_partsku_id", Objects.toString(r.getField("ten_partsku_id")));
                });

                result.put("ten_partsku_images", data);
                out.collect(result);
            }
        });

        DataSet<Map<String, Object>> skuSink = skuSet.leftOuterJoin(skuImgSet
        ).where(l -> l.get("ten_partsku_id").toString()
        ).equalTo(r -> r.get("ten_partsku_id").toString()
        ).with(new FlatJoinFunction<Map<String, Object>, Map<String, Object>, Map<String, Object>>() {
            @Override
            public void join(Map<String, Object> first, Map<String, Object> second, Collector<Map<String, Object>> out) throws Exception {
                if (second != null && second.get("ten_partsku_images") != null)
                    first.put("ten_partsku_images", second.get("ten_partsku_images"));
                else
                    first.put("ten_partsku_images", new ArrayList<>());

                first.put("_id", first.get("ten_partsku_id"));
                first.put("watermark", DateTimeUtils.getTimeStamp());
                out.collect(first);
            }
        });
        sink().elasticsearchSink(skuSink, indexPrefix + "sku" + indexSuffix);
    }

    /**
     * 标准码信息索引
     * @throws Exception
     */
    private void stdTrans() throws Exception {
        DataSet<Map<String, Object>> skuSet = source().jdbcSource(JdbcConnectionType.clickhouse,
                CqFinal.stdMsg.replace("${std_partsku_id}","")
        ).flatMap(new FlatMapFunction<Row, Map<String, Object>>() {
            @Override
            public void flatMap(Row value, Collector<Map<String, Object>> out) throws Exception {
                Map<String, Object> result = new HashMap<>();
                String[] valueArray = Objects.toString(value.getFieldAs("std_partsku_value"),"").split(",");
                List<Map<String, Object>> resultSkuVal = new ArrayList<>();
                if (valueArray.length > 0) {
                    for (String skuVal : valueArray) {
                        String[] skuKV = skuVal.split(":");
                        Map<String, Object> skuValueMsg = proUtil.get(skuKV[0]);
                        if (skuValueMsg != null) {
                            if (skuKV.length > 1)
                                skuValueMsg.put("std_partsku_value", skuKV[1]);
                            else
                                skuValueMsg.put("std_partsku_value", "");
                            resultSkuVal.add(skuValueMsg);
                        }
                    }
                    // resultSkuVal.sort(Comparator.comparing(t -> t.get("category_pro_index").toString()));

                }
                result.put("std_partsku_values", resultSkuVal);
                for (String col : Objects.requireNonNull(value.getFieldNames(true))) {
                    if (!col.equals("std_partsku_value"))
                        result.put(col, Objects.toString(value.getField(col)));
                }
                out.collect(result);
            }
        });

        DataSet<Map<String, Object>> skuImgSet = source().jdbcSource(JdbcConnectionType.clickhouse,
                CqFinal.stdImg.replace("${std_partsku_id}","")
        ).groupBy(r -> Objects.toString(r.getField("std_partsku_id"))
        ).reduceGroup(new GroupReduceFunction<Row, Map<String, Object>>() {
            @Override
            public void reduce(Iterable<Row> values, Collector<Map<String, Object>> out) throws Exception {
                Map<String, Object> result = new HashMap<>();
                List<Map<String, Object>> data = new ArrayList<>();

                values.forEach(r -> {
                    Map<String, Object> val = new HashMap<>();
                    for (String cloName : Objects.requireNonNull(r.getFieldNames(false))) {
                        val.put(cloName, Objects.toString(r.getField(cloName)));
                    }
                    val.remove("std_partsku_id");
                    data.add(val);
                    result.put("std_partsku_id", Objects.toString(r.getField("std_partsku_id")));
                });
                if (data.size() > 0)
                    result.put("std_partsku_images", data);
                out.collect(result);
            }
        });

        DataSet<Map<String, Object>> skuSink = skuSet.leftOuterJoin(skuImgSet
        ).where(l -> l.get("std_partsku_id").toString()
        ).equalTo(r -> r.get("std_partsku_id").toString()
        ).with(new FlatJoinFunction<Map<String, Object>, Map<String, Object>, Map<String, Object>>() {
            @Override
            public void join(Map<String, Object> first, Map<String, Object> second, Collector<Map<String, Object>> out) throws Exception {
                if (second != null)
                    first.put("std_partsku_images", second.get("std_partsku_images"));
                else
                    first.put("std_partsku_images", new ArrayList<>());

                first.put("_id", first.get("std_partsku_id"));
                first.put("watermark", DateTimeUtils.getTimeStamp());
                out.collect(first);
            }
        });
        sink().elasticsearchSink(skuSink, indexPrefix + "std" + indexSuffix);
    }

    /**
     * 行业码信息索引
     * @throws Exception
     */
    private void insTrans() throws Exception {
        DataSet<Map<String, Object>> skuSet = source().jdbcSource(JdbcConnectionType.clickhouse,
                CqFinal.insMsg.replace("${indus_partsku_id}","")
        ).flatMap(new FlatMapFunction<Row, Map<String, Object>>() {
            @Override
            public void flatMap(Row value, Collector<Map<String, Object>> out) throws Exception {
                Map<String, Object> result = new HashMap<>();
                String[] valueArray = Objects.toString(value.getFieldAs("indus_partsku_value"),"").split(",");
                List<Map<String, Object>> resultSkuVal = new ArrayList<>();
                if (valueArray.length > 0) {
                    for (String skuVal : valueArray) {
                        String[] skuKV = skuVal.split(":");
                        Map<String, Object> skuValueMsg = proUtil.get(skuKV[0]);
                        if (skuValueMsg != null) {
                            if (skuKV.length > 1)
                                skuValueMsg.put("indus_partsku_value", skuKV[1]);
                            else
                                skuValueMsg.put("indus_partsku_value", "");
                            resultSkuVal.add(skuValueMsg);
                        }
                    }
                    // resultSkuVal.sort(Comparator.comparing(t -> t.get("category_pro_index").toString()));

                }
                result.put("indus_partsku_values", resultSkuVal);

                result.put("indus_partsku_codes", CommonUtil.jsonAnalysis(value.getFieldAs("indus_partsku_code"), log));

                // 其他属性
                for (String col : Objects.requireNonNull(value.getFieldNames(true))) {
                    if (!col.equals("indus_partsku_value") && !col.equals("indus_partsku_code") )
                        result.put(col, Objects.toString(value.getField(col)));
                }
                out.collect(result);
            }
        });

        DataSet<Map<String, Object>> skuImgSet = source().jdbcSource(JdbcConnectionType.clickhouse,
                CqFinal.insImg.replace("${indus_partsku_id}","")
        ).groupBy(r -> Objects.toString(r.getField("indus_partsku_id"))
        ).reduceGroup(new GroupReduceFunction<Row, Map<String, Object>>() {
            @Override
            public void reduce(Iterable<Row> values, Collector<Map<String, Object>> out) throws Exception {
                Map<String, Object> result = new HashMap<>();
                List<Map<String, Object>> data = new ArrayList<>();

                values.forEach(r -> {
                    Map<String, Object> val = new HashMap<>();
                    for (String cloName : Objects.requireNonNull(r.getFieldNames(false))) {
                        val.put(cloName, Objects.toString(r.getField(cloName)));
                    }
                    val.remove("indus_partsku_id");
                    data.add(val);
                    result.put("indus_partsku_id", Objects.toString(r.getField("indus_partsku_id")));
                });
                if (data.size() > 0)
                    result.put("indus_partsku_images", data);
                out.collect(result);
            }
        });

        DataSet<Map<String, Object>> skuSink = skuSet.leftOuterJoin(skuImgSet
        ).where(l -> l.get("indus_partsku_id").toString()
        ).equalTo(r -> r.get("indus_partsku_id").toString()
        ).with(new FlatJoinFunction<Map<String, Object>, Map<String, Object>, Map<String, Object>>() {
            @Override
            public void join(Map<String, Object> first, Map<String, Object> second, Collector<Map<String, Object>> out) throws Exception {
                if (second != null)
                    first.put("indus_partsku_images", second.get("indus_partsku_images"));
                else
                    first.put("indus_partsku_images", new ArrayList<>());

                first.put("_id", first.get("indus_partsku_id"));
                first.put("watermark", DateTimeUtils.getTimeStamp());
                out.collect(first);
            }
        });
        sink().elasticsearchSink(skuSink, indexPrefix + "ins" + indexSuffix);
    }
    /**
     * 大厂码信息索引
     * @throws Exception
     */
    private void fmsTrans() throws Exception {
        DataSet<Map<String, Object>> skuSet = source().jdbcSource(JdbcConnectionType.clickhouse,
                CqFinal.fmsMsg.replace("${fms_partsku_id}","")
        ).flatMap(new FlatMapFunction<Row, Map<String, Object>>() {
            @Override
            public void flatMap(Row value, Collector<Map<String, Object>> out) throws Exception {
                Map<String, Object> result = new HashMap<>();
                String[] valueArray = Objects.toString(value.getFieldAs("fms_partsku_value"),"").split(",");
                List<Map<String, Object>> resultSkuVal = new ArrayList<>();
                if (valueArray.length > 0) {
                    for (String skuVal : valueArray) {
                        String[] skuKV = skuVal.split(":");
                        Map<String, Object> skuValueMsg = proUtil.get(skuKV[0]);
                        if (skuValueMsg != null) {
                            if (skuKV.length > 1)
                                skuValueMsg.put("fms_partsku_value", skuKV[1]);
                            else
                                skuValueMsg.put("fms_partsku_value", "");
                            resultSkuVal.add(skuValueMsg);
                        }
                    }
                    // resultSkuVal.sort(Comparator.comparing(t -> t.get("category_pro_index").toString()));
                }
                result.put("fms_partsku_values", resultSkuVal);

                result.put("fms_partsku_codes", CommonUtil.jsonAnalysis(value.getFieldAs("fms_partsku_code"), log));

                // 其他属性
                for (String col : Objects.requireNonNull(value.getFieldNames(true))) {
                    if (!col.equals("fms_partsku_value") && !col.equals("fms_partsku_code"))
                        result.put(col, Objects.toString(value.getField(col)));
                }
                out.collect(result);
            }
        });

        DataSet<Map<String, Object>> skuImgSet = source().jdbcSource(JdbcConnectionType.clickhouse,
                CqFinal.fmsImg.replace("${fms_partsku_id}","")
        ).groupBy(r -> Objects.toString(r.getField("fms_partsku_id"))
        ).reduceGroup(new GroupReduceFunction<Row, Map<String, Object>>() {
            @Override
            public void reduce(Iterable<Row> values, Collector<Map<String, Object>> out) throws Exception {
                Map<String, Object> result = new HashMap<>();
                List<Map<String, Object>> data = new ArrayList<>();

                values.forEach(r -> {
                    Map<String, Object> val = new HashMap<>();
                    for (String cloName : Objects.requireNonNull(r.getFieldNames(false))) {
                        val.put(cloName, Objects.toString(r.getField(cloName)));
                    }
                    val.remove("fms_partsku_id");
                    data.add(val);
                    result.put("fms_partsku_id", Objects.toString(r.getField("fms_partsku_id")));
                });
                if (data.size() > 0)
                    result.put("fms_partsku_images", data);
                out.collect(result);
            }
        });

        DataSet<Map<String, Object>> skuSink = skuSet.leftOuterJoin(skuImgSet
        ).where(l -> l.get("fms_partsku_id").toString()
        ).equalTo(r -> r.get("fms_partsku_id").toString()
        ).with(new FlatJoinFunction<Map<String, Object>, Map<String, Object>, Map<String, Object>>() {
            @Override
            public void join(Map<String, Object> first, Map<String, Object> second, Collector<Map<String, Object>> out) throws Exception {
                if (second != null)
                    first.put("fms_partsku_images", second.get("fms_partsku_images"));
                else
                    first.put("fms_partsku_images", new ArrayList<>());

                first.put("_id", first.get("fms_partsku_id"));
                first.put("watermark", DateTimeUtils.getTimeStamp());
                out.collect(first);
            }
        });
        sink().elasticsearchSink(skuSink, indexPrefix + "fms" + indexSuffix);
    }
    /**
     * oem码信息索引
     * @throws Exception
     */
    private void oemTrans() throws Exception {
        DataSet<Map<String, Object>> skuSet = source().jdbcSource(JdbcConnectionType.clickhouse,
                CqFinal.oemMsg.replace("${oem_partsku_id}","")
        ).flatMap(new FlatMapFunction<Row, Map<String, Object>>() {
            @Override
            public void flatMap(Row value, Collector<Map<String, Object>> out) throws Exception {
                Map<String, Object> result = new HashMap<>();
                String[] valueArray = Objects.toString(value.getFieldAs("oem_partsku_value"),"").split(",");
                List<Map<String, Object>> resultSkuVal = new ArrayList<>();
                if (valueArray.length > 0) {
                    for (String skuVal : valueArray) {
                        String[] skuKV = skuVal.split(":");
                        Map<String, Object> skuValueMsg = proUtil.get(skuKV[0]);
                        if (skuValueMsg != null) {
                            if (skuKV.length > 1)
                                skuValueMsg.put("oem_partsku_value", skuKV[1]);
                            else
                                skuValueMsg.put("oem_partsku_value", "");
                            resultSkuVal.add(skuValueMsg);
                        }
                    }
                    // resultSkuVal.sort(Comparator.comparing(t -> t.get("category_pro_index").toString()));
                }
                result.put("oem_partsku_values", resultSkuVal);

                result.put("oem_partsku_codes", CommonUtil.jsonAnalysis(value.getFieldAs("oem_partsku_code"), log));

                // 其他属性
                for (String col : Objects.requireNonNull(value.getFieldNames(true))) {
                    if (!col.equals("oem_partsku_value") && !col.equals("oem_partsku_code"))
                        result.put(col, Objects.toString(value.getField(col)));
                }
                out.collect(result);
            }
        });

        DataSet<Map<String, Object>> skuImgSet = source().jdbcSource(JdbcConnectionType.clickhouse,
                CqFinal.oemImg.replace("${oem_partsku_id}","")
        ).groupBy(r -> Objects.toString(r.getField("oem_partsku_id"))
        ).reduceGroup(new GroupReduceFunction<Row, Map<String, Object>>() {
            @Override
            public void reduce(Iterable<Row> values, Collector<Map<String, Object>> out) throws Exception {
                Map<String, Object> result = new HashMap<>();
                List<Map<String, Object>> data = new ArrayList<>();

                values.forEach(r -> {
                    Map<String, Object> val = new HashMap<>();
                    for (String cloName : Objects.requireNonNull(r.getFieldNames(false))) {
                        val.put(cloName, Objects.toString(r.getField(cloName)));
                    }
                    val.remove("oem_partsku_id");
                    data.add(val);
                    result.put("oem_partsku_id", Objects.toString(r.getField("oem_partsku_id")));
                });
                if (data.size() > 0)
                    result.put("oem_partsku_images", data);
                out.collect(result);
            }
        });

        DataSet<Map<String, Object>> skuSink = skuSet.leftOuterJoin(skuImgSet
        ).where(l -> l.get("oem_partsku_id").toString()
        ).equalTo(r -> r.get("oem_partsku_id").toString()
        ).with(new FlatJoinFunction<Map<String, Object>, Map<String, Object>, Map<String, Object>>() {
            @Override
            public void join(Map<String, Object> first, Map<String, Object> second, Collector<Map<String, Object>> out) throws Exception {
                if (second != null)
                    first.put("oem_partsku_images", second.get("oem_partsku_images"));
                else
                    first.put("oem_partsku_images", new ArrayList<>());

                first.put("_id", first.get("oem_partsku_id"));
                first.put("watermark", DateTimeUtils.getTimeStamp());
                out.collect(first);
            }
        });
        sink().elasticsearchSink(skuSink, indexPrefix + "oem" + indexSuffix);
    }
}
