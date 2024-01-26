package com.flinkuse.cases.external.bosch;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flinkuse.core.base.StreamApp;
import com.flinkuse.core.enums.JdbcConnectionType;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author learn
 * @date 2024/1/22 15:27
 */
public class GatherOutCh extends StreamApp {

    private final String cosObject;
    private final String skuUrl;
    private final String authorization;
    private final Integer skuNumber;

    public GatherOutCh(String[] args, String jobName) {
        super(args, jobName);
        cosObject = getScpsParams().get("bosch_cos_object");
        skuNumber = getScpsParams().getInt("bosch_querysku_nmber", 5);
        authorization = getScpsParams().get("bosch_authorization");
        skuUrl = getScpsParams().get("bosch_sku_url", "https://www.speed-part.com.cn/inter/spiderb/external/part/query");
    }

    public static void main(String[] args) throws Exception {
        new GatherOutCh(args, "GatherOutCh").start();
    }
    @Override
    public void run(StreamExecutionEnvironment streamEnv) {
        try {
            streamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);
//            DataStream<String> sor = cmSkuRelLogic(streamEnv
//            ).map(new MapFunction<String[], String>() {
//                @Override
//                public String map(String[] value) throws Exception {
//                    return value[3];
//                }
//            });
//
//            DataStream<String> skuRes =
//                    AsyncDataStream.orderedWait(prodGroupLogic(sor), new HttpClientAsyncFormat<>() {
//                                @Override
//                                public HttpRequestBase asyncInvokeInputHandle(List<String> o) throws Exception {
//                                    return boschProdApi(o);
//                                }
//                            }, 1, TimeUnit.MINUTES, 5);
//
//            skuMsgOut(skuRes);
            prodGroupLogic(streamEnv.fromElements("3397016430","3397016432","0092S47328","222","3333","44444")).print();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void skuMsgOut(DataStream<String> skuRes) {

        sink().jdbcSink(JdbcConnectionType.clickhouse, skuRes.flatMap(new FlatMapFunction<String, String[]>() {
            @Override
            public void flatMap(String value, Collector<String[]> out) throws Exception {
                Map<String, Object> skuData = JSONObject.parseObject(value, Map.class);
                if (skuData.get("code").equals(200)) {
                    List<Map<String, Object>> skuList = (List<Map<String, Object>>) skuData.get("data");
                    for (Map<String, Object> m : skuList) {
                        String[] res = new String[14];
                        res[0] = m.get("productNumber").toString();
                        res[1] = m.get("productKey").toString();
                        res[2] = m.get("enName").toString();
                        res[3] = m.get("zhName").toString();
                        res[4] = m.get("categoryCode").toString();
                        res[5] = m.get("category1Name").toString();
                        res[6] = m.get("featureList").toString();
                        res[7] = m.get("resourcesList").toString();
                        res[8] = m.get("oeInfo").toString();
                        res[9] = m.get("ceInfo").toString();
                        res[10] = m.get("pcdStatus").toString();
                        res[11] = m.get("pcdStatusCode").toString();
                        res[12] = m.get("alias").toString();
                        res[13] = m.get("isDeleted").toString();
                        out.collect(res);
                    }
                }
            }
        }), "INSERT INTO v1_scps_basics.ods_bosch_sku values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                , (JdbcStatementBuilder<String[]>) (ps, r) -> {
            ps.setString(1, r[0]);
            ps.setString(2, r[1]);
            ps.setString(3, r[2]);
            ps.setString(4, r[3]);
            ps.setString(5, r[4]);
            ps.setString(6, r[5]);
            ps.setString(7, r[6]);
            ps.setString(8, r[7]);
            ps.setString(9, r[8]);
            ps.setString(10, r[9]);
            ps.setString(11, r[10]);
            ps.setString(12, r[11]);
            ps.setString(13, r[12]);
            ps.setString(14, r[13]);
        });
    }

    private HttpRequestBase boschProdApi(List<String> o) {
        HttpPost httpPost = new HttpPost(skuUrl);
        httpPost.addHeader("Content-Type", "application/json");
        httpPost.addHeader("authorization", authorization);

        Map<String, List<String>> d = new HashMap<>();
        d.put("productNumber", o);
        StringEntity entity = new StringEntity(JSON.toJSONString(d), StandardCharsets.UTF_8);
        entity.setContentEncoding(StandardCharsets.UTF_8.toString());
        entity.setContentType("application/json");
        httpPost.setEntity(entity);

        return httpPost;
    }

    private DataStream<String[]> cmSkuRelLogic(StreamExecutionEnvironment streamEnv) throws Exception {

        DataStream<String[]> sor = source().cosSource(cosObject
        ).flatMap(new FlatMapFunction<String, String[]>() {
            @Override
            public void flatMap(String v, Collector<String[]> out) throws Exception {
                //uuid,boschId,sourceId,productNumber,productKey,appdes,coverStatusCode,coverStatus,remarkExternal,category1Name,category2Name,isDeleted,updateTime
                String[] res = v.split("\\|");
                if (res.length == 13)
                    out.collect(res);
                else if (res.length > 13){
                    String[] resArr = new String[13];
                    resArr[0] = res[0];
                    resArr[1] = res[1];
                    resArr[2] = res[2];
                    resArr[3] = res[3];
                    resArr[4] = res[4];
                    resArr[5] = res[5];
                    resArr[6] = res[6];
                    resArr[7] = res[7];

                    StringBuilder remark = new StringBuilder();
                    for (int i = 8; i<res.length-4; i++) {
                        remark.append(res[i]);
                    }
                    resArr[8] = remark.toString();

                    resArr[9] = res[res.length-4];
                    resArr[10] = res[res.length-3];
                    resArr[11] = res[res.length-2];
                    resArr[12] = res[res.length-1];
                    out.collect(resArr);
                }
            }
        });

        sink().jdbcSink(JdbcConnectionType.clickhouse, sor
                , "INSERT INTO v1_scps_basics.ods_bosch_cmskurel values(?,?,?,?,?,?,?,?,?,?,?,?,?)"
                , (JdbcStatementBuilder<String[]>) (ps, r) -> {
            ps.setString(1, r[0]);
            ps.setString(2, r[1]);
            ps.setString(3, r[2]);
            ps.setString(4, r[3]);
            ps.setString(5, r[4]);
            ps.setString(6, r[5]);
            ps.setString(7, r[6]);
            ps.setString(8, r[7]);
            ps.setString(9, r[8]);
            ps.setString(10, r[9]);
            ps.setString(11, r[10]);
            ps.setString(12, r[11]);
            ps.setString(13, r[12]);
        });

        return sor;
    }

    private DataStream<List<String>> prodGroupLogic(DataStream<String> inputStream) {

        return inputStream.keyBy(value -> value
        ).process(new KeyedProcessFunction<String, String, Tuple2<String, Integer>>() {
            /*
             状态编程
             去重
             */
            private ValueState<Boolean> seen;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化状态描述符
                ValueStateDescriptor<Boolean> stateDescriptor = new ValueStateDescriptor<>("seen", Boolean.class);
                seen = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 检查元素是否已经出现过
                if (seen.value() == null) {
                    // 如果是第一次出现，输出并标记为已出现
                    out.collect(Tuple2.of(value, 1));
                    seen.update(true);
                }
            }
        }).keyBy(k -> k.f1
        ).process(new KeyedProcessFunction<Integer, Tuple2<String, Integer>, Tuple2<String, Long>>() {
            /*
             状态编程
             给每个元素设置行号
             */
            private ValueState<Long> counterState;
            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化状态描述符
                ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("counterState", Long.class);
                counterState = getRuntimeContext().getState(stateDescriptor);
            }
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                Long currentCount = counterState.value();
                if (currentCount == null) {
                    currentCount = 0L;
                }
                // 更新计数器
                counterState.update(currentCount + 1);
                Tuple2<String, Long> t = Tuple2.of(value.f0, currentCount / skuNumber);
                out.collect(t);
            }
        }).keyBy(f -> f.f1
        ).countWindow(5
        ).apply(new RichWindowFunction<Tuple2<String, Long>, List<String>, Long, GlobalWindow>() {
            @Override
            public void apply(Long aLong, GlobalWindow window, Iterable<Tuple2<String, Long>> input, Collector<List<String>> out) throws Exception {
                List<String> res = new ArrayList<>();
                for (Tuple2<String, Long> t2 : input) {
                    res.add(t2.f0);
                }
                out.collect(res);
            }
        });
    }
}
