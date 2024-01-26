package com.flinkuse.cases.adapter.batch;

import com.flinkuse.core.base.BatchApp;
import com.flinkuse.core.enums.JdbcConnectionType;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CqSKUSTDToEs extends BatchApp {
    String SINK_ES_SKUSTD_INDEX;
    String STD_TREE_IDS;
    public CqSKUSTDToEs(String[] args) {
        super(args);
        SINK_ES_SKUSTD_INDEX = getScpsParams().get("SKUSTD_ES_INDEX_NAME", "category_query_stdsku");
        STD_TREE_IDS= getScpsParams().get("CATEGORY_QUERY_FILTER_TREE_ID");
    }

    public static void main(String[] args) {
        try {
            new CqSKUSTDToEs(args).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run(ExecutionEnvironment batchEnv) {
       // batchEnv.setParallelism(1);
        //获取sku和std rel表数据
        String relSql = " select tsr.category_id as category_id,\n" +
                " tsr.ten_partsku_id as ten_partsku_id,\n" +
                " tsr.std_partsku_id as std_partsku_id,\n" +
                " tsr.ten_category_id as ten_category_id,\n" +
                " tsr.ten_brand_id as ten_brand_id,\n" +
                " tst.std_tree_id as std_tree_id\n" +
                " FROM (\n" +
                "\t select category_id ,ten_partsku_id ,std_partsku_id ,ten_category_id ,ten_brand_id,std_tree_id\n" +
                "\t FROM scps_mysql_ods.t_tenant_std_rel ) as tsr\n" +
                " join(\n" +
                " \tselect category_id,std_tree_id\n" +
                " \tfrom scps_mysql_ods.t_std_tree) as tst\n" +
                " on tsr.category_id=tst.category_id\n" +
                " WHERE tsr.std_tree_id=tst.std_tree_id and tsr.std_tree_id not in ("+STD_TREE_IDS+")"
           ;
        //获取filter-oem表数据
        String filter_oemSql = "select GROUP_CONCAT(cast(oem_partsku_id as string)) as oem_partsku_ids,std_partsku_id,ten_partsku_id \n" +
                "from scps_mysql_ods.t_tenant_filter_oe \n" +
                "GROUP by ten_partsku_id,std_partsku_id";
        //获取filter_cm表数据
        String filter_cmSql = "SELECT std_partsku_id ,ten_partsku_id ,GROUP_CONCAT(CONCAT(CAST(oem_partsku_id as string),';',CAST(cm_id as string))) as filter_cmids\n" +
                "FROM scps_mysql_ods.t_tenant_filter_carmodel \n" +
                "group by std_partsku_id,ten_partsku_id\n";
        DataSet<Row>  source;
        DataSet<Row> filter_oem_source;
        DataSet<Row> filter_cm_source  ;
        try {
            source = source().jdbcSource(JdbcConnectionType.doris,relSql);
            filter_oem_source = source().jdbcSource(JdbcConnectionType.doris,filter_oemSql);
            filter_cm_source = source().jdbcSource(JdbcConnectionType.doris,filter_cmSql);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        DataSet<Map<String, Object>> relSource = source.map(new MapFunction<Row, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(Row row) throws Exception {
                Map<String, Object> map = new HashMap<>();
                map.put("category_id", String.valueOf(row.getField("category_id")));
                map.put("ten_partsku_id", String.valueOf(row.getField("ten_partsku_id")));
                map.put("std_partsku_id", String.valueOf(row.getField("std_partsku_id")));
                map.put("ten_category_id", String.valueOf(row.getField("ten_category_id")));
                map.put("ten_brand_id", String.valueOf(row.getField("ten_brand_id")));
                map.put("std_tree_id", String.valueOf(row.getField("std_tree_id")));
                String _id = row.getField("std_partsku_id").toString() + ";" + row.getField("ten_partsku_id").toString();
                map.put("_id", _id);

                return map;
            }
        });

        DataSet<Map<String, Object>> filter_oemids = filter_oem_source.map(new MapFunction<Row, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(Row row) throws Exception {
                Map<String, Object> map = new HashMap<>();
                map.put("oem_partsku_ids", String.valueOf(row.getField("oem_partsku_ids")).split(","));
                map.put("_id", row.getField("std_partsku_id").toString() + ";" + row.getField("ten_partsku_id").toString());
                return map;
            }
        });

        DataSet<Map<String, Object>> filter_cmids = filter_cm_source.map(new MapFunction<Row, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(Row row) throws Exception {
                Map<String, Object> map = new HashMap<>();
                map.put("_id", row.getField("std_partsku_id").toString() + ";" + row.getField("ten_partsku_id").toString());
                String[] split = row.getField("filter_cmids").toString().split(",");
                List<Map<String, Object>> list = new ArrayList<>();
                for (String s : split) {
                    Map<String, Object> filter_cmids = new HashMap<>();
                    String[] result = s.split(";");
                    filter_cmids.put("oem_partsku_id", result[0]);
                    filter_cmids.put("cm_id", result[1]);
                    list.add(filter_cmids);
                }
                map.put("filter_cmids", list);
                return map;
            }
        });
        DataSet<Map<String, Object>> result = relSource.leftOuterJoin(filter_oemids, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND)
                .where(map -> (String) map.get("_id"))
                .equalTo(map -> (String) map.get("_id"))
                .with(new JoinFunction<Map<String, Object>, Map<String, Object>, Map<String, Object>>() {
                    @Override
                    public Map<String, Object> join(Map<String, Object> map, Map<String, Object> map2) throws Exception {
                        if (map != null && map2 != null) {
                            map.putAll(map2);
                        }
                        //log.info("第一次join数据 {}",map);
                        return map;

                    }
                }).leftOuterJoin(filter_cmids, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND)
                .where(map -> (String) map.get("_id"))
                .equalTo(map -> (String) map.get("_id"))
                .with(new JoinFunction<Map<String, Object>, Map<String, Object>, Map<String, Object>>() {
                    @Override
                    public Map<String, Object> join(Map<String, Object> map, Map<String, Object> map2) throws Exception {
                        if (map != null && map2 != null) {
                            map.putAll(map2);
                        }
                        //log.info("第二次join数据 {}",map);
                        return map;
                    }
                });
        sink().elasticsearchSink(result, SINK_ES_SKUSTD_INDEX);
        //result.writeAsText("E://text");

    }
}
