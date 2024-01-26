package com.flinkuse.cases.cqcal.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONValidator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Map;

/**
 * @author learn
 * @date 2023/12/19 11:13
 */
public class CommonUtil {
    public static Object jsonAnalysis(String js, Logger log) {
        try {
            if (js == null || js.equals(""))
                return new ArrayList<>();
            if (js.contains("\\")) {
                if (!js.contains("\\\""))
                    js = js.replace("\\", "\\\\");
            }
            if (JSONValidator.from(js).validate()) {
                return JSON.parseArray(js, Map.class);
            } else {
                log.error("字符串({})是不json串", js);
            }
        } catch (Exception je) {
            log.error("字符串({})解析json异常:{}", js, je.getMessage());
        }
        return new ArrayList<>();
    }
    /**
     * 获取产品属性配置
     * 获取应用车型std_tree_id配置
     * @throws Exception
     */
    public static Tuple3<Map<String, String[]>
                , Map<String, Map<String, Object>>
                , Map<String, Tuple2<String, String>>> dataInit(Configuration cf, String tables) {
        try {
            InitConfUtil cu = new InitConfUtil(cf);

            Tuple3<Map<String, String[]>
                    , Map<String, Map<String, Object>>
                    , Map<String, Tuple2<String, String>>>
                    t3 = Tuple3.of(cu.getStdTreeId()
                    , cu.getCategoryPro()
                    , cu.getTableColumn(tables));

            cu.chClose();

            return t3;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
