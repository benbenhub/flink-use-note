package com.flinkuse.cases.code.batch;

import com.flinkuse.cases.code.sql.BatchSql;
import com.github.houbb.pinyin.bs.PinyinBs;
import com.github.houbb.pinyin.constant.enums.PinyinStyleEnum;
import com.github.houbb.pinyin.support.style.PinyinToneStyles;
import com.flinkuse.core.base.BatchApp;
import com.flinkuse.core.enums.JdbcConnectionType;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class BasicsCodeQuery extends BatchApp {

    private String addIndex;
    private int parallelism;

    public BasicsCodeQuery(String[] args) {
        super(args);
    }

    public static void main(String[] args) throws Exception {
        BasicsCodeQuery bcq = new BasicsCodeQuery(args);
        bcq.start();
    }

    @Override
    public void run(ExecutionEnvironment batchEnv) {
        //传索引名
        this.addIndex = getScpsParams().get("addIndex");
        this.parallelism = getScpsParams().getInt("parallelism");
        try {
            //carmodel();
            product();
            fms();
            ins();
            oem();
            dealer();
            sku();
            exchange();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void carmodel() throws Exception {
        //获取ck数据
        DataSet<Map<String, Object>> cmDataSet = getSource(BatchSql.cmQuerySql).flatMap(new FlatMapFunction<Row, Map<String,Object>>() {
            @Override
            public void flatMap(Row row, Collector<Map<String,Object>> collector) throws Exception{
                HashMap<String, Object> map = new HashMap<>();
                map.put("cm_brand", row.getField("cm_brand"));
                map.put("cm_engine_model", row.getField("cm_engine_model"));
                map.put("cm_ids", Objects.toString(row.getField("cm_ids")).split(","));
                collector.collect(map);
            }}).name("cm");
        sink().elasticsearchSink(cmDataSet,"basics_code"+addIndex);
    }

    public void product() throws Exception {

        DataSet<Map<String, Object>> productSet = getSource( BatchSql.productSql).flatMap(new FlatMapFunction<Row, Map<String,Object>>() {
            @Override
            public void flatMap(Row row, Collector<Map<String,Object>> collector) throws Exception{
                HashMap<String, Object> map = new HashMap<>();
                map.put("dealer_partsku_id", Objects.toString(row.getField("dealer_partsku_id"),""));
                map.put("dealer_partsku_code",Objects.toString(row.getField("dealer_partsku_code"),""));
                map.put("dealer_partsku_external_code",Objects.toString(row.getField("dealer_partsku_external_code"),""));
                map.put("dealer_partsku_order_url",Objects.toString(row.getField("dealer_partsku_order_url"),""));
                map.put("tenant_id",Objects.toString(row.getField("tenant_id"),""));
                //map.put("ten_sys_id",Objects.toString(row.getField("ten_sys_id"),""));
                map.put("dealer_category_id",Objects.toString(row.getField("dealer_category_id"),""));
                map.put("dealer_part_id",Objects.toString(row.getField("dealer_part_id"),""));
                map.put("agent_tenant_id",Objects.toString(row.getField("agent_tenant_id"),""));
                map.put("agent_ten_brand_id",Objects.toString(row.getField("agent_ten_brand_id"),""));
                map.put("agent_ten_category_id",Objects.toString(row.getField("agent_ten_category_id"),""));
                map.put("agent_ten_part_id",Objects.toString(row.getField("agent_ten_part_id"),""));
                map.put("agent_ten_partsku_code",Objects.toString(row.getField("agent_ten_partsku_code"),""));
                map.put("category_id",Objects.toString(row.getField("category_id"),""));
                collector.collect(map);
            }}).name("product");

        sink().elasticsearchSink(productSet,"basics_code"+addIndex);
    }

    public void fms() throws Exception {
        DataSet<Map<String, Object>> fmsSet = getSource( BatchSql.fmsSql).flatMap(new FlatMapFunction<Row, Map<String,Object>>() {
            @Override
            public void flatMap(Row row, Collector<Map<String,Object>> collector) throws Exception{
                HashMap<String, Object> map = new HashMap<>();
                map.put("fms_partsku_id", Objects.toString(row.getField("fms_partsku_id"),""));
                map.put("fms_partsku_desc",Objects.toString(row.getField("fms_partsku_desc"),""));
                map.put("fms_partsku_desc_img",Objects.toString(row.getField("fms_partsku_desc_img"),""));
                map.put("fms_partsku_status",Objects.toString(row.getField("fms_partsku_status"),""));
                map.put("category_id",Objects.toString(row.getField("category_id"),""));
//                map.put("fms_part_id",Objects.toString(row.getField("fms_part_id"),""));
                map.put("fms_brand_id",Objects.toString(row.getField("fms_brand_id"),""));
                map.put("fms_category_id",Objects.toString(row.getField("fms_category_id"),""));
                map.put("fms_partsku_code_id",Objects.toString(row.getField("fms_partsku_code_id"),""));
                map.put("fms_partsku_code",Objects.toString(row.getField("fms_partsku_code"),""));
                map.put("fms_partsku_fmt_code",Objects.toString(row.getField("fms_partsku_fmt_code"),""));
                collector.collect(map);
            }}).name("fms_sku");

        sink().elasticsearchSink(fmsSet,"basics_code"+addIndex);
    }

    public void ins() throws Exception {
        DataSet<Map<String, Object>> insSet = getSource( BatchSql.insSql).flatMap(new FlatMapFunction<Row, Map<String,Object>>() {
            @Override
            public void flatMap(Row row, Collector<Map<String,Object>> collector) throws Exception{
                HashMap<String, Object> map = new HashMap<>();
                map.put("indus_partsku_id", Objects.toString(row.getField("indus_partsku_id"),""));
                map.put("indus_partsku_desc",Objects.toString(row.getField("indus_partsku_desc"),""));
                map.put("indus_partsku_desc_img",Objects.toString(row.getField("indus_partsku_desc_img"),""));
                map.put("indus_partsku_status",Objects.toString(row.getField("indus_partsku_status"),""));
                map.put("category_id",Objects.toString(row.getField("category_id"),""));
//                map.put("indus_part_id",Objects.toString(row.getField("indus_part_id"),""));
                map.put("indus_brand_id",Objects.toString(row.getField("indus_brand_id"),""));
                map.put("indus_category_id",Objects.toString(row.getField("indus_category_id"),""));

                map.put("indus_partsku_code_id",Objects.toString(row.getField("indus_partsku_code_id") , ""));
                map.put("indus_partsku_code",Objects.toString(row.getField("indus_partsku_code"), ""));
                map.put("indus_partsku_fmt_code",Objects.toString(row.getField("indus_partsku_fmt_code"),""));
                collector.collect(map);
            }}).name("ins_sku");

        sink().elasticsearchSink(insSet,"basics_code"+addIndex);
    }

    public void oem() throws Exception {
        DataSet<Map<String, Object>> oemSet = getSource( BatchSql.oemSql).flatMap(new FlatMapFunction<Row, Map<String,Object>>() {
            @Override
            public void flatMap(Row row, Collector<Map<String,Object>> collector) throws Exception{
                HashMap<String, Object> map = new HashMap<>();
                map.put("oem_partsku_id", Objects.toString(row.getField("oem_partsku_id"),""));
                map.put("carmodel_brand_id",Objects.toString(row.getField("carmodel_brand_id"),""));
                map.put("std_tree_id",Objects.toString(row.getField("std_tree_id"),""));
                map.put("oem_partsku_desc",Objects.toString(row.getField("oem_partsku_desc"),""));
                map.put("oem_partsku_desc_img",Objects.toString(row.getField("oem_partsku_desc_img"),""));
                map.put("oem_partsku_status",Objects.toString(row.getField("oem_partsku_status"),""));
                map.put("create_origin",Objects.toString(row.getField("create_origin"),""));
                map.put("create_origin_id",Objects.toString(row.getField("create_origin_id"),""));
                map.put("create_type",Objects.toString(row.getField("create_type"),""));
                map.put("oem_partsku_code_id",Objects.toString(row.getField("oem_partsku_code_id") , ""));
                map.put("oem_partsku_code",Objects.toString(row.getField("oem_partsku_code") , ""));
                map.put("oem_partsku_fmt_code",Objects.toString(row.getField("oem_partsku_fmt_code") , ""));
                collector.collect(map);
            }}).setParallelism(parallelism).name("oem_sku");

        sink().elasticsearchSink(oemSet,"basics_code"+addIndex);
    }

    public void dealer() throws Exception {
        DataSet<Map<String, Object>> dealerSet = getSource( BatchSql.dealerSql).flatMap(new FlatMapFunction<Row, Map<String,Object>>() {
            @Override
            public void flatMap(Row row, Collector<Map<String,Object>> collector) throws Exception{

                String prodSkuName = Objects.toString(row.getField("prod_sku_name"),"");
                String prodSkuNameStr = prodSkuName.toLowerCase(Locale.ROOT);
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

                HashMap<String, Object> map = new HashMap<>();
                map.put("prod_sku_id", Objects.toString(row.getField("prod_sku_id"),""));
                map.put("prod_group_id",Objects.toString(row.getField("prod_group_id"),""));

                map.put("prod_sku_name",prodSkuName);
                map.put("prod_sku_name_py_first",prodSkuNamePyFirst);
                map.put("prod_sku_name_py",prodSkuNamePy);

                String prodSkuCode = Objects.toString(row.getField("prod_sku_code"),"");
                String prodSkuCodeStr = prodSkuCode.toLowerCase(Locale.ROOT);
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
                map.put("prod_sku_code", prodSkuCode);
                map.put("prod_sku_code_py_first",prodSkuCodePyFirst);
                map.put("prod_sku_code_py",prodSkuCodePy);

                map.put("dealer_partsku_id",Objects.toString(row.getField("dealer_partsku_id"),""));
                map.put("prod_sku_desc",Objects.toString(row.getField("prod_sku_desc"),""));
                map.put("prod_sku_desc_img",Objects.toString(row.getField("prod_sku_desc_img"),""));
                map.put("prod_sku_origin_price",Objects.toString(row.getField("prod_sku_origin_price"),""));
                map.put("prod_sku_stock",Objects.toString(row.getField("prod_sku_stock"),""));

                map.put("prod_sku_spec",Objects.toString(row.getField("prod_sku_spec"),""));
                map.put("prod_sku_weight",Objects.toString(row.getField("prod_sku_weight"),""));
                map.put("prod_sku_volume",Objects.toString(row.getField("prod_sku_volume"),""));
                map.put("prod_unit_id",Objects.toString(row.getField("prod_unit_id"),""));
                map.put("prod_sku_status",Objects.toString(row.getField("prod_sku_status"),""));
                map.put("prod_sku_min_purchase",Objects.toString(row.getField("prod_sku_min_purchase"),""));

                map.put("dealer_partsku_code",Objects.toString(row.getField("dealer_partsku_code"),""));
                map.put("dealer_partsku_external_code",Objects.toString(row.getField("dealer_partsku_external_code"),""));
                map.put("dealer_partsku_order_url",Objects.toString(row.getField("dealer_partsku_order_url"),""));

                map.put("tenant_id",Objects.toString(row.getField("tenant_id"),""));
                map.put("ten_sys_id",Objects.toString(row.getField("ten_sys_id"),""));
                map.put("dealer_category_id",Objects.toString(row.getField("dealer_category_id"),""));
                map.put("dealer_part_id",Objects.toString(row.getField("dealer_part_id"),""));
                map.put("agent_tenant_id",Objects.toString(row.getField("agent_tenant_id"),""));
                map.put("agent_ten_brand_id",Objects.toString(row.getField("agent_ten_brand_id"),""));
                map.put("agent_ten_category_id",Objects.toString(row.getField("agent_ten_category_id"),""));
                map.put("agent_ten_part_id",Objects.toString(row.getField("agent_ten_part_id"),""));
                map.put("agent_ten_partsku_code",Objects.toString(row.getField("agent_ten_partsku_code"),""));
                map.put("category_id",Objects.toString(row.getField("category_id"),""));

                map.put("prod_spu_id",Objects.toString(row.getField("prod_spu_id"),""));
                map.put("prod_sku_brand_name",Objects.toString(row.getField("prod_sku_brand_name"),""));
                map.put("ten_partsku_code",Objects.toString(row.getField("ten_partsku_code"),""));
                map.put("ten_brand_id",Objects.toString(row.getField("ten_brand_id"),""));
                map.put("ten_part_id",Objects.toString(row.getField("ten_part_id"),""));
                map.put("ten_partsku_id",Objects.toString(row.getField("ten_partsku_id"),""));
                collector.collect(map);
            }}).name("prod_sku");

        sink().elasticsearchSink(dealerSet,"basics_code"+addIndex);
    }

    public void sku() throws Exception {
        DataSet<Map<String, Object>> skuSet = getSource( BatchSql.skuSql).flatMap(new FlatMapFunction<Row, Map<String,Object>>() {
            @Override
            public void flatMap(Row row, Collector<Map<String,Object>> collector) throws Exception{
                HashMap<String, Object> map = new HashMap<>();
                map.put("ten_partsku_id", Objects.toString(row.getField("ten_partsku_id"),""));
                map.put("ten_partsku_code",Objects.toString(row.getField("ten_partsku_code"),""));
                map.put("ten_partsku_external_code",Objects.toString(row.getField("ten_partsku_external_code"),""));
                map.put("ten_partsku_feature",Objects.toString(row.getField("ten_partsku_feature"),""));
                map.put("ten_partsku_order_url",Objects.toString(row.getField("ten_partsku_order_url"),""));
                map.put("ten_partsku_course_url",Objects.toString(row.getField("ten_partsku_course_url"),""));
                map.put("tenant_id",Objects.toString(row.getField("tenant_id"),""));
                map.put("ten_brand_id",Objects.toString(row.getField("ten_brand_id"),""));
                map.put("ten_category_id",Objects.toString(row.getField("ten_category_id"),""));
                map.put("ten_part_id",Objects.toString(row.getField("ten_part_id"),""));
                map.put("ten_partsku_desc",Objects.toString(row.getField("ten_partsku_desc"),""));
                map.put("ten_partsku_desc_img",Objects.toString(row.getField("ten_partsku_desc_img"),""));
                map.put("category_id",Objects.toString(row.getField("category_id"),""));
                collector.collect(map);
            }}).setParallelism(parallelism).name("ten_sku");

        sink().elasticsearchSink(skuSet,"basics_code"+addIndex);
    }

    public void exchange() throws Exception {
        DataSet<Map<String, Object>> exchangeSet = getSource( BatchSql.exchangeSql).flatMap(new FlatMapFunction<Row, Map<String,Object>>() {
            @Override
            public void flatMap(Row row, Collector<Map<String,Object>> collector) throws Exception{
                HashMap<String, Object> map = new HashMap<>();
                map.put("partsku_exchange_id", Objects.toString(row.getField("partsku_exchange_id"),""));
                map.put("partsku_exchange_brand_id",Objects.toString(row.getField("partsku_exchange_brand_id"),""));
                map.put("partsku_exchange_code",Objects.toString(row.getField("partsku_exchange_code"),""));
                map.put("partsku_exchange_fmt_code",Objects.toString(row.getField("partsku_exchange_fmt_code"),""));
                map.put("partsku_exchange_type",Objects.toString(row.getField("partsku_exchange_type"),""));
                map.put("tenant_id",Objects.toString(row.getField("tenant_id"),""));
                map.put("ten_brand_id",Objects.toString(row.getField("ten_brand_id"),""));
                map.put("ten_category_id",Objects.toString(row.getField("ten_category_id"),""));
                map.put("ten_part_id",Objects.toString(row.getField("ten_part_id"),""));
                map.put("ten_partsku_id",Objects.toString(row.getField("ten_partsku_id"),""));
                map.put("category_id",Objects.toString(row.getField("category_id"),""));
                collector.collect(map);
            }}).setParallelism(parallelism).name("sku_exchange");

        //输出到es
        sink().elasticsearchSink(exchangeSet,"basics_code"+addIndex);
    }

    public DataSet<Row> getSource(Tuple2<String,String> t2) throws Exception {
        return source().jdbcSource(JdbcConnectionType.clickhouse, t2.f0);
    }
}
