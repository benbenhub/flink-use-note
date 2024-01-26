package com.flinkuse.cases.adapter.batch;

import com.flinkuse.cases.adapter.constant.CqFinal;
import com.flinkuse.cases.adapter.entity.CategoryQueryCmoemstd;
import com.flinkuse.cases.adapter.entity.CategoryQueryStdsku;
import com.flinkuse.core.base.BatchApp;
import com.flinkuse.core.enums.JdbcConnectionType;
import com.flinkuse.core.util.DateTimeUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * @author learn
 * @date 2023/6/26 10:58
 */
public class CqIndexInit extends BatchApp {

    private final JdbcConnectionType jct;
    private final String oemIndex;
    private final String stdIndex;
    private String tenantId;

    public CqIndexInit(String[] args, String jobName) {
        super(args, jobName);
        oemIndex = getScpsParams().get("cq_init_oemindex", CqFinal.esIndexCmOemStd);
        stdIndex = getScpsParams().get("cq_init_stdindex", CqFinal.esIndexStdSku);
        tenantId = getScpsParams().get("cq_init_tenantid", "");

        if (getScpsParams().get("cq_init_dbtype","doris").equals("clickhouse"))
            jct = JdbcConnectionType.clickhouse;
        else
            jct = JdbcConnectionType.doris;
    }

    public static void main(String[] args) {
        try {
            new CqIndexInit(args, "category query init").start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run(ExecutionEnvironment batchEnv) {
        cmOemStd();
        stdSku();
    }
    private void cmOemStd() {
        String sql ;
        switch (jct) {
            case clickhouse:
                sql = CqFinal.ch_joinCmOemStdSql;
                break;
            case doris:
            default:
                sql = CqFinal.joinCmOemStdSql;
                break;
        }
        DataSet<CategoryQueryCmoemstd> so = source().jdbcSource(jct
                , sql.replace("${oem_partsku_id}", "")
                , new RowTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO)
        ).flatMap(new FlatMapFunction<Row, CategoryQueryCmoemstd>() {
            @Override
            public void flatMap(Row r, Collector<CategoryQueryCmoemstd> out) throws Exception {
//                a.std_tree_id,             0
//                a.oem_partsku_id,          1
//                a.cm_id,                   2
//                a.oem_carmodel_status,     3
//                a.oem_carmodel_remark_id,  4
//                b.std_partsku_id           5
                CategoryQueryCmoemstd cq = new CategoryQueryCmoemstd();
                cq.setCreate_time(DateTimeUtils.dateNowFormat());
                cq.setCm_id(Objects.toString(r.getField(2)));
                cq.setOem_partsku_id(Objects.toString(r.getField(1)));
                cq.setStd_partsku_id(Objects.toString(r.getField(5)));
                cq.setStd_tree_id(Objects.toString(r.getField(0)));
                cq.setOem_carmodel_remark_id(Objects.toString(r.getField(4)));
                cq.setWatermark("0");
                out.collect(cq);
            }
        });

        sink().elasticsearchSink(so, oemIndex);
    }

    private void stdSku() {
        String sql ;
        switch (jct) {
            case clickhouse:
                sql = CqFinal.ch_joinCmSkuSql;
                if (!tenantId.equals(""))
                    tenantId = " tenant_id GLOBAL in (" + tenantId + ") and ";
                break;
            case doris:
            default:
                sql = CqFinal.joinCmSkuSql;
                if (!tenantId.equals(""))
                    tenantId = " tenant_id in (" + tenantId + ") and ";
                break;
        }
//        DataSet<CqStdskuEs> ss = source().jdbcSource(jct
        DataSet<CategoryQueryStdsku> ss = source().jdbcSource(jct
                , sql.replace("${ten_partsku_id}", tenantId)
                , new RowTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO)
        ).flatMap(new FlatMapFunction<Row, CategoryQueryStdsku>() {
            @Override
            public void flatMap(Row r, Collector<CategoryQueryStdsku> out) throws Exception {
//  a.tenant_id,             0
//	a.ten_brand_id,          1
//	a.ten_category_id,       2
//	a.category_id,           3
//	a.std_tree_id,           4
//	a.std_partsku_id,        5
//	a.oem_partsku_id,        6
//	a.ten_partsku_id,        7
//	a.cm_id,                 8
//	a.ten_cm_comment,        9
//	b.oem_carmodel_remark_id 10
                CategoryQueryStdsku ss = new CategoryQueryStdsku();
                ss.setCategory_id(Objects.toString(r.getField(3), ""));
                ss.setCreate_time(DateTimeUtils.dateNowFormat());
                ss.setTen_category_id(Objects.toString(r.getField(2), ""));
                ss.setStd_partsku_id(Objects.toString(r.getField(5), ""));
                ss.setTen_partsku_id(Objects.toString(r.getField(7), ""));
                ss.setTen_brand_id(Objects.toString(r.getField(1), ""));
                ss.setStd_tree_id(Objects.toString(r.getField(4), ""));
                ss.setWatermark("0");
                ss.setTenant_id(Objects.toString(r.getField(0), ""));
                ss.setOem_partsku_id(Objects.toString(r.getField(6), ""));
                ss.setCm_id(Objects.toString(r.getField(8), ""));
                ss.setTen_cm_comment(Objects.toString(r.getField(9), ""));
                ss.setOem_carmodel_remark_id(Objects.toString(r.getField(10), ""));
                out.collect(ss);
            }
        });
//                .groupBy(CategoryQueryStdsku::getTen_partsku_id
//        ).reduceGroup(new GroupReduceFunction<CategoryQueryStdsku, CqStdskuEs>() {
//            @Override
//            public void reduce(Iterable<CategoryQueryStdsku> values, Collector<CqStdskuEs> out) throws Exception {
//                CqStdskuEs esData = new CqStdskuEs();
//                List<CqStdskuEs.Rel> relList = new ArrayList<>();
//                for (CategoryQueryStdsku c : values) {
//                    esData.setCategory_id(c.getCategory_id());
//                    esData.setCreate_time(c.getCreate_time());
//                    esData.setTen_partsku_id(c.getTen_partsku_id());
//                    esData.setTen_brand_id(c.getTen_brand_id());
//                    esData.setTen_category_id(c.getTen_category_id());
//                    esData.setWatermark(c.getWatermark());
//                    esData.setTenant_id(c.getTenant_id());
//
//                    CqStdskuEs.Rel r = new CqStdskuEs.Rel();
//                    r.setStd_partsku_id(c.getStd_partsku_id());
//                    r.setStd_tree_id(c.getStd_tree_id());
//                    r.setOem_partsku_id(c.getOem_partsku_id());
//                    r.setCm_id(c.getCm_id());
//                    r.setTen_cm_comment(c.getTen_cm_comment());
//                    r.setOem_catmodel_remark_id(c.getOem_carmodel_remark_id());
//                    relList.add(r);
//                }
//                esData.setRel(relList);
//                out.collect(esData);
//            }
//        });
        sink().elasticsearchSink(ss, stdIndex);
    }
}
