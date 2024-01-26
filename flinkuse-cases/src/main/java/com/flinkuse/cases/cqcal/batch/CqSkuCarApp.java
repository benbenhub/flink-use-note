package com.flinkuse.cases.cqcal.batch;

import com.flinkuse.core.base.BatchApp;
import com.flinkuse.core.enums.JdbcConnectionType;
import com.flinkuse.core.util.DateTimeUtils;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author learn
 * @date 2024/1/25 14:47
 */
public class CqSkuCarApp extends BatchApp {

    private final String indexPrefix;
    private final String indexSuffix;

    public CqSkuCarApp(String[] args, String jobName) {
        super(args, jobName);
        indexPrefix = getScpsParams().get("cq_index_prefix", "basics_cq_");
        indexSuffix = getScpsParams().get("cq_SkuCarIndex_suffix", "_" + DateTimeUtils.dateNowFormat("yyyyMMddHHmmss"));
    }

    public static void main(String[] args) throws Exception {
        new CqSkuCarApp(args, "CqSkuCarApp").start();
    }
    @Override
    public void run(ExecutionEnvironment batchEnv) {
        try {
            DataSet<Row> cm = source().jdbcSource(JdbcConnectionType.clickhouse,"SELECT\n\tcm_id,\n\tcm_brand,\n\tcm_model,\n\tcm_displacement,\n\tvisitParamExtractString(cm_carmodel,'cm_sales_year') as cm_sales_year,\n\tvisitParamExtractString(cm_carmodel,'cm_stop_year') as cm_stop_year,\n\tvisitParamExtractString(cm_carmodel,'cm_engine_model') as cm_engine_model,\n\tvisitParamExtractString(cm_carmodel,'cm_brand_letter') as cm_brand_letter,\n\tvisitParamExtractString(cm_carmodel,'cm_model_letter') as cm_model_letter\nFROM\n\tv1_scps_carmodel.ods_t_carmodel_base_rt final\nWHERE \n\tdata_flag > 0");
            DataSet<Row> cmSkuRel = source().jdbcSource(JdbcConnectionType.clickhouse,"SELECT DISTINCT cm_id,ten_partsku_id FROM scps_mysql_ods.t_tenant_cm_rel final where data_flag > 0");

            DataSet<Map<String, Object>> out = cmSkuRel.join(cm
            ).where(r -> Objects.toString(r.getField("cm_id"))
            ).equalTo(r -> Objects.toString(r.getField("cm_id"))
            ).with(new FlatJoinFunction<Row, Row, Map<String, Object>>() {
                @Override
                public void join(Row f, Row s, Collector<Map<String, Object>> out) throws Exception {
                    Map<String, Object> res = new HashMap<>();
                    for (String c : Objects.requireNonNull(s.getFieldNames(false))) {
                        res.put(c, s.getField(c));
                    }
                    res.put("ten_partsku_id", Objects.toString(f.getField("ten_partsku_id")));
                    out.collect(res);
                }
            }).groupBy(m -> m.get("ten_partsku_id").toString() + m.get("cm_brand") + m.get("cm_model") + m.get("cm_displacement")
                    + m.get("cm_sales_year") + m.get("cm_stop_year") + m.get("cm_brand_letter") + m.get("cm_model_letter")
            ).reduceGroup(new GroupReduceFunction<Map<String, Object>, Map<String, Object>>() {
                @Override
                public void reduce(Iterable<Map<String, Object>> values, Collector<Map<String, Object>> out) throws Exception {
                    Map<String, Object> res = new HashMap<>();
                    StringBuilder cm_engine_model = new StringBuilder();
                    String cm_displacement = "", cm_sales_year = "", cm_stop_year = "";
                    for (Map<String, Object> m : values) {
                        cm_engine_model.append(m.get("cm_engine_model")).append(",");
                        cm_displacement = m.get("cm_displacement").toString();
                        cm_sales_year = m.get("cm_sales_year").toString();
                        cm_stop_year = m.get("cm_stop_year").toString();

                        res.put("ten_partsku_id", m.get("ten_partsku_id"));
                        res.put("cm_brand", m.get("cm_brand"));
                        res.put("cm_model", m.get("cm_model"));
                        res.put("cm_brand_letter", m.get("cm_brand_letter"));
                        res.put("cm_model_letter", m.get("cm_model_letter"));
                    }
                    cm_engine_model.delete(cm_engine_model.length()-1, cm_engine_model.length());
                    res.put("cm_pro_desc", cm_displacement + "(" + cm_sales_year + "-" + cm_stop_year + ") 发动机型号: " + cm_engine_model);
                    out.collect(res);
                }
            });

            sink().elasticsearchSink(out, indexPrefix + "skucarmodel" + indexSuffix);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
