package com.flinkuse.cases.adapter.constant;

/**
 * 任务常量类包含sql
 * @author learn
 * @date 2023/5/17 13:50
 */
public class CqFinal {
    /**
     * es索引
     */
    public static final String esIndexStdSku = "category_query_stdsku";
    /**
     * es索引
     */
    public static final String esIndexCmOemStd = "category_query_cmoemstd";
    /**
     * 此任务所用到的binlog表
     */
    public static final String cmOemStdTables = "t_oem_carmodel_rel,t_standard_oem_rel";

    public static final String skuTables = "t_tenant_cm_rel,t_tenant_std_rel,t_tenant_filter_carmodel,t_tenant_filter_oe";

    public static String joinCmOemStdSql = "SELECT\n\ta.std_tree_id,\n\ta.oem_partsku_id,\n\ta.cm_id,\n\ta.oem_carmodel_status,\n\ta.oem_carmodel_remark_id,\n\tb.std_partsku_id\nFROM\n\t(\n\tselect\n\t\tstd_tree_id,\n\t\toem_partsku_id,\n\t\tcm_id,\n\t\toem_carmodel_status,\n\t\toem_carmodel_remark_id\n\tfrom\n\t\tscps_mysql_ods.t_oem_carmodel_rel\n\tWHERE\n\t\t${oem_partsku_id} std_tree_id IN (SELECT std_tree_id FROM scps_mysql_ods.t_std_tree where category_id IN (SELECT category_id FROM scps_mysql_ods.t_category WHERE category_new_type = 'VERSION' and data_flag > 0))\n\t\tand data_flag > 0) a\nINNER JOIN \n(\n\tselect\n\t\tstd_partsku_id,\n\t\toem_partsku_id\n\tfrom\n\t\tscps_mysql_ods.t_standard_oem_rel\n\tWHERE\n\t\t${oem_partsku_id} std_tree_id IN (SELECT std_tree_id FROM scps_mysql_ods.t_std_tree where category_id IN (SELECT category_id FROM scps_mysql_ods.t_category WHERE category_new_type = 'VERSION' and data_flag > 0))\n\t\tand data_flag > 0) b ON\n\ta.oem_partsku_id = b.oem_partsku_id";

    // public static String joinStdSkuSql = "SELECT\n\tc.*,\n\td.filter_cm_id,\n\te.filter_oem_id\nFROM\n(\n\tselect\n\t\ttenant_id,\n\t\tten_brand_id,\n\t\tten_category_id,\n\t\tten_partsku_id,\n\t\tstd_partsku_id,\n\t\tbrand_category_id,\n\t\tten_part_id,\n\t\tcategory_id\n\tfrom\n\t\tscps_mysql_ods.t_tenant_std_rel\n\tWHERE\n\t\tten_partsku_id IN (${ten_partsku_id}) and data_flag > 0) c \nLEFT JOIN \n(\n\tselect\n\t\tgroup_concat(CONCAT(cast(cm_id as varchar),',',cast(oem_partsku_id as varchar))) as filter_cm_id,\n\t\tten_partsku_id\n\tfrom\n\t\tscps_mysql_ods.t_tenant_filter_carmodel\n\tWHERE\n\t\tten_partsku_id IN (${ten_partsku_id}) and data_flag > 0 GROUP BY ten_partsku_id) d ON\n\td.ten_partsku_id = c.ten_partsku_id\nLEFT JOIN \n(\n\tselect\n\t\tgroup_concat(cast(oem_partsku_id as varchar)) as filter_oem_id,\n\t\tten_partsku_id\n\tfrom\n\t\tscps_mysql_ods.t_tenant_filter_oe\n\tWHERE\n\t\tten_partsku_id IN (${ten_partsku_id}) and data_flag > 0 GROUP BY ten_partsku_id) e ON\n\te.ten_partsku_id = c.ten_partsku_id";

    public static String joinCmSkuSql = "SELECT\n\ta.tenant_id,\n\ta.ten_brand_id,\n\ta.ten_category_id,\n\ta.category_id,\n\ta.std_tree_id,\n\ta.std_partsku_id,\n\ta.oem_partsku_id,\n\ta.ten_partsku_id,\n\ta.cm_id,\n\ta.ten_cm_comment,\n\tb.oem_carmodel_remark_id\nfrom\n\t(\n\tSELECT\n\t\ttenant_id,\n\t\tten_brand_id,\n\t\tten_category_id,\n\t\tcategory_id,\n\t\tstd_tree_id,\n\t\tstd_partsku_id,\n\t\toem_partsku_id,\n\t\tten_partsku_id,\n\t\tcm_id,\n\t\tten_cm_comment\n\tFROM\n\t\tscps_mysql_ods.t_tenant_cm_rel\n\twhere\n\t\t${ten_partsku_id} data_flag > 0) a\nLEFT JOIN (\n\tSELECT\n\t\tcm_id,\n\t\toem_partsku_id,\n\t\toem_carmodel_remark_id\n\tFROM\n\t\tscps_mysql_ods.t_oem_carmodel_rel\n\tWHERE\n\t\t(cm_id) in (\n\t\tSELECT\n\t\t\tcm_id\n\t\tFROM\n\t\t\tscps_mysql_ods.t_tenant_cm_rel\n\t\twhere\n\t\t\t${ten_partsku_id} data_flag > 0)\n\t\tand data_flag > 0) b \non\n\ta.oem_partsku_id = b.oem_partsku_id\n\tand a.cm_id = b.cm_id";


    public static String ch_joinCmOemStdSql = "SELECT\n\ta.std_tree_id,\n\ta.oem_partsku_id,\n\ta.cm_id,\n\ta.oem_carmodel_status,\n\ta.oem_carmodel_remark_id,\n\tb.std_partsku_id\nFROM\n\t(\n\tselect\n\t\tstd_tree_id,\n\t\toem_partsku_id,\n\t\tcm_id,\n\t\toem_carmodel_status,\n\t\toem_carmodel_remark_id\n\tfrom\n\t\tscps_mysql_ods.t_oem_carmodel_rel final\n\tWHERE\n\t\t${oem_partsku_id} std_tree_id GLOBAL IN (SELECT std_tree_id FROM scps_mysql_ods.t_std_tree final where category_id GLOBAL IN (SELECT category_id FROM scps_mysql_ods.t_category final WHERE category_new_type = 'VERSION' and data_flag > 0))\n\t\tand data_flag > 0) a\nGLOBAL INNER JOIN \n(\n\tselect\n\t\tstd_partsku_id,\n\t\toem_partsku_id\n\tfrom\n\t\tscps_mysql_ods.t_standard_oem_rel final\n\tWHERE\n\t\t${oem_partsku_id} std_tree_id GLOBAL IN (SELECT std_tree_id FROM scps_mysql_ods.t_std_tree final where category_id GLOBAL IN (SELECT category_id FROM scps_mysql_ods.t_category final WHERE category_new_type = 'VERSION' and data_flag > 0))\n\t\tand data_flag > 0) b ON\n\ta.oem_partsku_id = b.oem_partsku_id";

    public static String ch_joinCmSkuSql = "SELECT\n\ta.tenant_id,\n\ta.ten_brand_id,\n\ta.ten_category_id,\n\ta.category_id,\n\ta.std_tree_id,\n\ta.std_partsku_id,\n\ta.oem_partsku_id,\n\ta.ten_partsku_id,\n\ta.cm_id,\n\ta.ten_cm_comment,\n\tb.oem_carmodel_remark_id\nfrom\n\t(\n\tSELECT\n\t\ttenant_id,\n\t\tten_brand_id,\n\t\tten_category_id,\n\t\tcategory_id,\n\t\tstd_tree_id,\n\t\tstd_partsku_id,\n\t\toem_partsku_id,\n\t\tten_partsku_id,\n\t\tcm_id,\n\t\tten_cm_comment\n\tFROM\n\t\tscps_mysql_ods.t_tenant_cm_rel final\n\twhere\n\t\t${ten_partsku_id} data_flag > 0) a\nGLOBAL LEFT JOIN (\n\tSELECT\n\t\tcm_id,\n\t\toem_partsku_id,\n\t\tstd_tree_id,\n\t\toem_carmodel_remark_id\n\tFROM\n\t\tscps_mysql_ods.t_oem_carmodel_rel final\n\tWHERE\n\t\tcm_id GLOBAL in (\n\t\tSELECT\n\t\t\tcm_id\n\t\tFROM\n\t\t\tscps_mysql_ods.t_tenant_cm_rel final\n\t\twhere\n\t\t\t${ten_partsku_id} data_flag > 0)\n\t\tand data_flag > 0) b \non\n\ta.oem_partsku_id = b.oem_partsku_id\n\tand a.cm_id = b.cm_id and a.std_tree_id=b.std_tree_id";

}
