package com.flinkuse.cases.adapter.trans;

import com.alibaba.fastjson.JSONObject;
import com.flinkuse.cases.adapter.constant.CqFinal;
import com.flinkuse.cases.adapter.entity.CategoryQueryCmoemstd;
import com.flinkuse.cases.adapter.entity.CategoryQueryStdsku;
import com.flinkuse.cases.common.AdapterEsEntity;
import com.flinkuse.core.connector.elasticsearch7.Elasticsearch7AsyncFormat;
import com.flinkuse.core.connector.jdbc.JdbcAsyncFormat;
import com.flinkuse.core.connector.jdbc.JdbcStatementFunction;
import com.flinkuse.core.enums.JdbcConnectionType;
import com.flinkuse.core.enums.SqlOperate;
import com.flinkuse.core.util.DateTimeUtils;
import org.apache.flink.connector.elasticsearch.sink.RequestIndexer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author learn
 * @date 2023/5/31 14:44
 */
public class CqTransformImpl extends CqTransform {

    private final int capacity;

    public CqTransformImpl(String[] args, String jobName) {
        super(args, jobName);
        odsTopicGroup = getScpsParams().get("cq_ods_topic_group", "cq-group");
        windowsMinutes = getScpsParams().getLong("cq_windows_time", 5L);
        windowsSeconds = getScpsParams().getLong("cq_windows_time_seconds", 10L);
        offset = getScpsParams().get("cq_offset","latest");
        capacity = getScpsParams().getInt("cq_capacity", 5);
        windowsCount = getScpsParams().getLong("cq_windows_count", 100L);
        oemIndex = getScpsParams().get("cq_init_oemindex", CqFinal.esIndexCmOemStd);
        stdIndex = getScpsParams().get("cq_init_stdindex", CqFinal.esIndexStdSku);
    }

    @Override
    public void skuDeleteIndexer(Object data, RequestIndexer indexer, long wm) {
        List<String> t2 = (List<String>) data;
        log.info("[sku]删除,水印:{},数量:{}", wm, t2.size());
        for (String s : t2) {
            indexer.add(Requests.deleteRequest(stdIndex).id(s));
        }
    }

    @Override
    public void skuInsertIndexer(Object data, RequestIndexer indexer, long wm) {
        List<Map<String,Object>> d = (List<Map<String, Object>>) data;
        log.info("[sku]增加,水印:{},新品类数量：{}", wm, d.size());

        for (Map<String,Object> cqd : d) {
            CategoryQueryStdsku ss = new CategoryQueryStdsku();
            ss.setCategory_id(Objects.toString(cqd.get("category_id"), "0"));
            ss.setCreate_time(DateTimeUtils.dateNowFormat());
            ss.setTen_category_id(Objects.toString(cqd.get("ten_category_id"), "0"));
            ss.setStd_partsku_id(Objects.toString(cqd.get("std_partsku_id"), "0"));
            ss.setTen_partsku_id(Objects.toString(cqd.get("ten_partsku_id"), "0"));
            ss.setTen_brand_id(Objects.toString(cqd.get("ten_brand_id"), "0"));
            ss.setStd_tree_id(Objects.toString(cqd.get("std_tree_id"), "0"));
            ss.setWatermark(Objects.toString(wm));
            ss.setTenant_id(Objects.toString(cqd.get("tenant_id"), "0"));
            ss.setOem_partsku_id(Objects.toString(cqd.get("oem_partsku_id"), "0"));
            ss.setCm_id(Objects.toString(cqd.get("cm_id"), "0"));
            ss.setTen_cm_comment(Objects.toString(cqd.get("ten_cm_comment"), "0"));
            ss.setOem_carmodel_remark_id(Objects.toString(cqd.get("oem_carmodel_remark_id"), "0"));

            indexer.add(Requests.indexRequest()
                    .index(stdIndex)
                    .source(JSONObject.toJSONString(ss), XContentType.JSON));
        }
//
//        Map<String, List<Map<String, Object>>> g = Linq.of(d).groupBy(cqd -> Objects.toString(cqd.get("ten_category_id"), "0")
//                + Objects.toString(cqd.get("ten_partsku_id"), "0")
//        ).stream().collect(Collectors.toMap(IGrouping::getKey, IEnumerable::toList));
//
//        for (Map.Entry<String, List<Map<String, Object>>> e : g.entrySet()) {
//            CqStdskuEs esData = new CqStdskuEs();
//            List<CqStdskuEs.Rel> relList = new ArrayList<>();
//            for (Map<String, Object> c : e.getValue()) {
//                esData.setCategory_id(Objects.toString(c.get("category_id"), "0"));
//                esData.setCreate_time(DateTimeUtils.dateNowFormat());
//                esData.setTen_partsku_id(Objects.toString(c.get("ten_partsku_id"), "0"));
//                esData.setTen_brand_id(Objects.toString(c.get("ten_brand_id"), "0"));
//                esData.setTen_category_id(Objects.toString(c.get("ten_category_id"), "0"));
//                esData.setWatermark(Objects.toString(wm));
//                esData.setTenant_id(Objects.toString(c.get("tenant_id"), "0"));
//
//                CqStdskuEs.Rel r = new CqStdskuEs.Rel();
//                r.setStd_partsku_id(Objects.toString(c.get("std_partsku_id"), "0"));
//                r.setStd_tree_id(Objects.toString(c.get("std_tree_id"), "0"));
//                r.setOem_partsku_id(Objects.toString(c.get("oem_partsku_id"), "0"));
//                r.setCm_id(Objects.toString(c.get("cm_id"), "0"));
//                r.setTen_cm_comment(Objects.toString(c.get("ten_cm_comment"), "0"));
//                r.setOem_catmodel_remark_id(Objects.toString(c.get("oem_catmodel_remark_id"), "0"));
//                relList.add(r);
//            }
//            esData.setRel(relList);
//            indexer.add(Requests.indexRequest()
//                    .index(CqFinal.esIndexStdSku)
//                    .source(JSONObject.toJSONString(esData), XContentType.JSON));
//        }
    }

    @Override
    public DataStream<AdapterEsEntity> skuFindData(DataStream<String> sourceFindIds) {
        return AsyncDataStream.orderedWait(sourceFindIds
            , new JdbcAsyncFormat<String, AdapterEsEntity>(JdbcConnectionType.clickhouse) {
                @Override
                public AdapterEsEntity asyncInvokeInputHandle(String i, JdbcStatementFunction q) throws Exception {
                    AdapterEsEntity a = new AdapterEsEntity();
//                    String sqlStdSku = CqFinal.joinStdSkuSql.replace("${ten_partsku_id}", i);
                    String sqlCmSku = CqFinal.ch_joinCmSkuSql.replace("${ten_partsku_id}", "ten_partsku_id in (" + i + ") and ");

//                    a.setData(Tuple2.of(q.runQuery(sqlStdSku), q.runQuery(sqlCmSku)));
                    a.setData(q.runQuery(sqlCmSku));
                    a.setAction(SqlOperate.insert);

                    return a;
                }

                @Override
                public void timeout(String input, ResultFuture<AdapterEsEntity> resultFuture) {
                    // super.timeout(input, resultFuture);
                    log.error("查找所有适配数据时超时:{}", input);
                    resultFuture.completeExceptionally(new RuntimeException("查找所有适配数据时超时!"));
                }
            }
            , windowsMinutes
            , TimeUnit.MINUTES
            , capacity
        ).name("query tables");
    }

    @Override
    public DataStream<AdapterEsEntity> skuFindEsDeleteData(DataStream<String> sourceFindIds) {
        return AsyncDataStream.orderedWait(sourceFindIds
            , new Elasticsearch7AsyncFormat<String, AdapterEsEntity>() {
                @Override
                public void asyncInvoke(String in, ResultFuture<AdapterEsEntity> out) throws Exception {
                    TermsQueryBuilder qbTen;
                    qbTen = QueryBuilders.termsQuery("ten_partsku_id", in.split(","));

                    BoolQueryBuilder bqbStdSku = new BoolQueryBuilder();
                    bqbStdSku.should(qbTen);

                    List<String> stdSkuList = getFunction().searchId(stdIndex, bqbStdSku);

                    AdapterEsEntity a = new AdapterEsEntity();
                    a.setData(stdSkuList);
                    a.setAction(SqlOperate.delete);

                    out.complete(Collections.singleton(a));
                }

                @Override
                public void timeout(String in, ResultFuture<AdapterEsEntity> out) {
                    log.error("查找要删除的es数据时超时:{}", in);
                    out.completeExceptionally(new RuntimeException("查找要删除的es数据时超时!"));
                }
            }
            , windowsMinutes
            , TimeUnit.MINUTES
            , capacity
        ).name("query es delete data");
    }

    @Override
    public void oemDeleteIndexer(Object data, RequestIndexer indexer, long wm) {
        List<String> t2 = (List<String>) data;
        log.info("[oem]删除,水印:{},数量:{}", wm, t2.size());
        for (String s : t2) {
            indexer.add(Requests.deleteRequest(oemIndex).id(s));
        }
    }

    @Override
    public void oemInsertIndexer(Object data, RequestIndexer indexer, long wm) {
        List<Map<String,Object>> d = (List<Map<String,Object>>) data;
        log.info("[oem]增加,水印:{},数量:{}", wm, d.size());
        for (Map<String,Object> cqd : d) {
            // 四个id都有值说明关联关系成立 生成增加数据
            String cmId = Objects.toString(cqd.get("cm_id"), "")
                    , oemId = Objects.toString(cqd.get("oem_partsku_id"), "")
                    , stdId = Objects.toString(cqd.get("std_partsku_id"), "");
            // 构造增加数据
            if (!cmId.isEmpty() && !oemId.isEmpty() && !stdId.isEmpty()) {
                CategoryQueryCmoemstd cq = new CategoryQueryCmoemstd();
                cq.setCreate_time(DateTimeUtils.dateNowFormat());
                cq.setCm_id(cmId);
                cq.setOem_partsku_id(oemId);
                cq.setStd_partsku_id(stdId);
                cq.setStd_tree_id(Objects.toString(cqd.get("std_tree_id"), "0"));
                cq.setOem_carmodel_remark_id(Objects.toString(cqd.get("oem_carmodel_remark_id"), "0"));
                cq.setWatermark(Objects.toString(wm));
                indexer.add(Requests.indexRequest()
                        .index(oemIndex)
                        .source(JSONObject.toJSONString(cq), XContentType.JSON));
            }
        }
    }

    @Override
    public DataStream<AdapterEsEntity> oemFindData(DataStream<String> sourceFindIds) {
        return AsyncDataStream.orderedWait(sourceFindIds
            , new JdbcAsyncFormat<String, AdapterEsEntity>(JdbcConnectionType.clickhouse) {
                @Override
                public AdapterEsEntity asyncInvokeInputHandle(String i, JdbcStatementFunction q) throws Exception {

                    String sqlCmOemStd = CqFinal.ch_joinCmOemStdSql.replace("${oem_partsku_id}", "(oem_partsku_id IN (" + i + ")) AND ");

                    AdapterEsEntity a = new AdapterEsEntity();
                    a.setData(q.runQuery(sqlCmOemStd));
                    a.setAction(SqlOperate.insert);
                    return a;
                }

                @Override
                public void timeout(String input, ResultFuture<AdapterEsEntity> resultFuture) {
                    log.error("查找所有适配数据时超时:{}", input);
                    resultFuture.completeExceptionally(new RuntimeException("查找所有适配数据时超时!"));
                }
            }
            , windowsMinutes
            , TimeUnit.MINUTES
            , capacity
        ).name("query tables");
    }

    @Override
    public DataStream<AdapterEsEntity> oemFindEsDeleteData(DataStream<String> sourceFindIds) {
        return AsyncDataStream.orderedWait(sourceFindIds
            , new Elasticsearch7AsyncFormat<String, AdapterEsEntity>() {
                @Override
                public void asyncInvoke(String in, ResultFuture<AdapterEsEntity> out) throws Exception {
                    TermsQueryBuilder qbOem;
                    qbOem = QueryBuilders.termsQuery("oem_partsku_id", in.split(","));

                    BoolQueryBuilder bqbCmOemStd = new BoolQueryBuilder();
                    bqbCmOemStd.should(qbOem);

                    List<String> cmOemStdList = getFunction().searchId(oemIndex, bqbCmOemStd);
                    AdapterEsEntity a = new AdapterEsEntity();
                    a.setData(cmOemStdList);
                    a.setAction(SqlOperate.delete);

                    out.complete(Collections.singleton(a));
                }

                @Override
                public void timeout(String in, ResultFuture<AdapterEsEntity> out) {
                    log.error("查找要删除的es数据时超时:{}", in);
                    out.completeExceptionally(new RuntimeException("查找要删除的es数据时超时!"));
                }
            }
            , windowsMinutes
            , TimeUnit.MINUTES
            , capacity
        ).name("query es delete data");
    }
}
