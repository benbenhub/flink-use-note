package com.flinkuse.cases.cqcal.util;

import com.alibaba.fastjson.JSON;
import com.flinkuse.cases.cqcal.constant.CqFinal;
import com.flinkuse.core.util.ClickHouseDBOPUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author learn
 * @date 2023/6/16 11:04
 */
public class InitConfUtil {

    private final ClickHouseDBOPUtil ch;

    public InitConfUtil(Configuration scpsConfig) throws Exception {
        ch = new ClickHouseDBOPUtil(scpsConfig);
    }

    public Map<String, Map<String, Object>> getCategoryPro() throws Exception {

        List<Map<String,Object>> matchRel =  ch.queryFunction().runQuery(CqFinal.categoryPro);

        // 构造map 分别是group map；by map
        Map<String, Map<String, Object>> groupMap = new HashMap<>();
        for (Map<String, Object> matchData : matchRel) {
            String id = matchData.get("category_pro_id").toString();
            matchData.remove("category_pro_id");
            groupMap.put(id, matchData);
        }
        return groupMap;
    }

    public Map<String, String[]> getStdTreeId() throws Exception {

        List<Map<String,Object>> matchRel =  ch.queryFunction(
        ).runQuery("SELECT std_tree_cm_conf_id, groupArray(toString(std_tree_id)) std_tree_ids from scps_mysql_ods.t_std_tree_carmodel_conf_rel final WHERE data_flag > 0 GROUP BY std_tree_cm_conf_id\n");

        // 构造map 分别是group map；by map
        Map<String, String[]> groupMap = new HashMap<>();

        for (Map<String, Object> matchData : matchRel) {
            String id = matchData.get("std_tree_cm_conf_id").toString();
            groupMap.put(id, (String[]) matchData.get("std_tree_ids"));
        }

        return groupMap;
    }
    public Map<String, Tuple2<String, String>> getTableColumn(String tables) throws Exception {
        StringBuilder wc = new StringBuilder();
        for (String dt : tables.split(";")) {
            wc.append("'").append(dt).append("',");
        }
        wc.delete(wc.length() - 1, wc.length());
        List<Map<String,Object>> tb = ch.queryFunction(
        ).runQuery("select\n"
                + "\ta.*,\n"
                + "\tb.name as k\n"
                + "FROM\n"
                + "\t(\n"
                + "\tselect\n"
                + "\t\t`table`,\n"
                + "\t\tarrayStringConcat(groupArray(name),\n"
                + "\t\t',') column_name\n"
                + "\tfrom\n"
                + "\t\tsystem.columns\n"
                + "\twhere\n"
                + "\t\tdatabase = 'scps_mysql_ods'\n"
                + "\t\tAND `table` GLOBAL IN ("+wc
                + ")\n"
                + "\t\tAND name != 'data_flag'\n"
                + "\tGROUP BY\n"
                + "\t\t`table`) a\n"
                + "JOIN \n"
                + "(\n"
                + "\tselect\n"
                + "\t\t`table`,\n"
                + "\t\tname\n"
                + "\tfrom\n"
                + "\t\tsystem.columns\n"
                + "\twhere\n"
                + "\t\tdatabase = 'scps_mysql_ods'\n"
                + "\t\tAND `table` GLOBAL IN (" + wc
                + ")\n"
                + "\t\t\tAND `position` = 1) b on\n"
                + "\ta.`table` = b.`table`");
        Map<String, Tuple2<String, String>> result = new HashMap<>();
        for (Map<String,Object> mm : tb) {
            result.put(mm.get("table").toString(), Tuple2.of(mm.get("k").toString(), mm.get("column_name").toString()));
        }
        return result;
    }

    public Map<String, Tuple2<String[], List<String>>> getStdTreeIdPro() throws Exception {

        List<Map<String,Object>> matchRel =  ch.queryFunction(
        ).runQuery("select\n\ttid.std_tree_cm_conf_id,\n\ttid.std_tree_ids,\n\tpro.std_tree_cm_conf_detail\nFROM\n\t(\n\tSELECT\n\t\tstd_tree_cm_conf_id,\n\t\tgroupArray(toString(std_tree_id)) std_tree_ids\n\tfrom\n\t\tscps_mysql_ods.t_std_tree_carmodel_conf_rel final\n\tWHERE\n\t\tdata_flag > 0\n\tGROUP BY\n\t\tstd_tree_cm_conf_id) as tid\nJOIN (\n\tselect\n\t\tstd_tree_cm_conf_id,\n\t\tstd_tree_cm_conf_detail\n\tfrom\n\t\tscps_mysql_ods.t_std_tree_carmodel_conf final\n\tWHERE\n\t\tdata_flag > 0) as pro\non\n\ttid.std_tree_cm_conf_id = pro.std_tree_cm_conf_id"
        );

        // 构造map 分别是group map；by map
        Map<String, Tuple2<String[], List<String>>> groupMap = new HashMap<>();

        for (Map<String, Object> matchData : matchRel) {
            String id = matchData.get("std_tree_cm_conf_id").toString();
            List<String> drsb = new ArrayList<>();
            for (Object mm : JSON.parseObject(matchData.get("std_tree_cm_conf_detail").toString(), List.class)) {
                Map<String, String> md = (Map<String, String>) mm;
                drsb.add(md.get("column"));
            }
            groupMap.put(id, Tuple2.of((String[]) matchData.get("std_tree_ids"), drsb));
        }

        return groupMap;
    }

    public void chClose() {
        ch.close();
    }
}
