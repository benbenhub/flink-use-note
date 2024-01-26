package com.flinkuse.cases.code.sql;

import org.apache.flink.api.java.tuple.Tuple2;

public class BatchSql {

    public static Tuple2<String,String> cmQuerySql = Tuple2.of("SELECT\n" +
                    "cm_brand,\n" +
                    "cm_engine_model,\n" +
                    "arrayStringConcat(groupUniqArray(toString(cm_id)),',') cm_ids\n" +
                    "FROM scps_mysql_ods.t_carmodel_base final \n" +
                    "WHERE cm_engine_model<>'' and data_flag > 0\n" +
                    "GROUP BY cm_brand,cm_engine_model",

            "cm_brand,cm_engine_model,cm_ids");

    public static Tuple2<String,String> productSql = Tuple2.of(
            "select dealer_partsku_id\n" +
                    ",dealer_partsku_code\n" +
                    ",dealer_partsku_external_code\n" +
                    ",dealer_partsku_order_url\n" +
                    ",tenant_id\n" +
                    ",dealer_category_id\n" +
                    ",dealer_part_id\n" +
                    ",agent_tenant_id\n" +
                    ",agent_ten_brand_id\n" +
                    ",agent_ten_category_id\n" +
                    ",agent_ten_part_id\n" +
                    ",agent_ten_partsku_code\n" +
                    ",category_id from \n" +
                    "scps_mysql_ods.t_dealer_partsku final\n" +
                    "where data_flag > 0",

            "dealer_partsku_id,dealer_partsku_code,dealer_partsku_external_code,dealer_partsku_order_url,tenant_id,dealer_category_id,dealer_part_id,agent_tenant_id,agent_ten_brand_id,agent_ten_category_id,agent_ten_part_id,agent_ten_partsku_code,category_id"
    );

    public static Tuple2<String,String> fmsSql = Tuple2.of("select a.fms_partsku_id\n" +
                    ",a.fms_partsku_desc\n" +
                    ",a.fms_partsku_desc_img\n" +
                    ",a.fms_partsku_status\n" +
                    ",a.category_id\n" +
                    ",a.fms_brand_id\n" +
                    ",a.fms_category_id\n" +
                    ",b.fms_partsku_code_id\n" +
                    ",b.fms_partsku_code\n" +
                    ",b.fms_partsku_fmt_code\n" +
                    "from ( select fms_partsku_id\n" +
                    ",fms_partsku_desc\n" +
                    ",fms_partsku_desc_img\n" +
                    ",fms_partsku_status\n" +
                    ",category_id\n" +
                    ",fms_brand_id\n" +
                    ",fms_category_id\n" +
                    "from scps_mysql_ods.t_famous_partsku final where data_flag > 0 )a\n" +
                    "global left join \n" +
                    "(select fms_partsku_id,fms_partsku_code_id,fms_partsku_code,fms_partsku_fmt_code\n" +
                    "from scps_mysql_ods.t_famous_partsku_code final where data_flag > 0 )b \n" +
                    "on a.fms_partsku_id = b.fms_partsku_id \n" +
                    "",

            "fms_partsku_id,fms_partsku_desc,fms_partsku_desc_img,fms_partsku_status,category_id,fms_brand_id,fms_category_id,fms_partsku_code_id,fms_partsku_code,fms_partsku_fmt_code");

    public static Tuple2<String,String> insSql = Tuple2.of("select a.indus_partsku_id\n" +
                    ",a.indus_partsku_desc\n" +
                    ",a.indus_partsku_desc_img\n" +
                    ",a.indus_partsku_status\n" +
                    ",a.category_id\n" +
                    ",a.indus_brand_id\n" +
                    ",a.indus_category_id\n" +
                    ",b.indus_partsku_code_id \n" +
                    ",b.indus_partsku_code \n" +
                    ",b.indus_partsku_fmt_code \n" +
                    "from (select indus_partsku_id,indus_partsku_desc,indus_partsku_desc_img,indus_partsku_status,category_id," +
                    "indus_brand_id,indus_category_id from scps_mysql_ods.t_industry_partsku final\n" +
                    " where data_flag > 0) a\n" +
                    " global left join\n" +
                    " (select indus_partsku_id,indus_partsku_code_id,indus_partsku_code,indus_partsku_fmt_code from scps_mysql_ods.t_industry_partsku_code final\n" +
                    " where data_flag > 0) b \n" +
                    "on a.indus_partsku_id = b.indus_partsku_id ",

            "indus_partsku_id,indus_partsku_desc,indus_partsku_desc_img,indus_partsku_status,category_id,indus_brand_id,indus_category_id,indus_partsku_code_id,indus_partsku_code,indus_partsku_fmt_code");

    public static Tuple2<String,String> oemSql = Tuple2.of("select a.oem_partsku_id\n" +
                    ",a.carmodel_brand_id \n" +
                    ",a.std_tree_id \n" +
                    ",a.oem_partsku_desc \n" +
                    ",a.oem_partsku_desc_img \n" +
                    ",a.oem_partsku_status \n" +
                    ",a.create_origin \n" +
                    ",a.create_origin_id \n" +
                    ",a.create_type \n" +
                    ",b.oem_partsku_code_id \n" +
                    ",b.oem_partsku_code \n" +
                    ",b.oem_partsku_fmt_code \n" +
                    "from (select oem_partsku_id,carmodel_brand_id,std_tree_id,oem_partsku_desc,oem_partsku_desc_img," +
                    "oem_partsku_status,create_origin,create_origin_id,create_type\n" +
                    "from scps_mysql_ods.t_oem_partsku final\n" +
                    "where data_flag > 0) a \n" +
                    "global left join\n" +
                    "(select oem_partsku_id,oem_partsku_code_id,oem_partsku_code,oem_partsku_fmt_code\n" +
                    "from scps_mysql_ods.t_oem_partsku_code final\n" +
                    "where data_flag > 0) b on a.oem_partsku_id = b.oem_partsku_id ",

            "oem_partsku_id,std_tree_id,category_id,oem_partsku_desc,oem_partsku_desc_img,oem_partsku_status,create_origin,create_origin_id,create_type,oem_partsku_code_id,oem_partsku_code,oem_partsku_fmt_code");

    public static Tuple2<String,String> dealerSql = Tuple2.of("select\n"
                    + "a.prod_sku_id\n"
                    + ",a.prod_group_id\n"
                    + ",a.prod_sku_name\n"
                    + ",a.prod_sku_code\n"
                    + ",a.dealer_partsku_id\n"
                    + ",a.prod_sku_desc\n"
                    + ",a.prod_sku_desc_img\n"
                    + ",a.prod_sku_origin_price\n"
                    + ",a.prod_sku_stock\n"
                    + ",a.prod_sku_spec\n"
                    + ",a.prod_spu_id,a.prod_sku_brand_name,a.ten_partsku_code\n"
                    + ",a.prod_sku_weight\n"
                    + ",a.prod_sku_volume\n"
                    + ",a.prod_unit_id\n"
                    + ",a.prod_sku_status\n"
                    + ",a.prod_sku_min_purchase\n"
                    + ",b.dealer_partsku_code\n"
                    + ",b.dealer_partsku_external_code\n"
                    + ",b.dealer_partsku_order_url\n"
                    + ",b.tenant_id\n"
                    + ",a.ten_sys_id\n"
                    + ",b.dealer_category_id\n"
                    + ",b.dealer_part_id\n"
                    + ",b.agent_tenant_id\n"
                    + ",b.agent_ten_brand_id\n"
                    + ",b.agent_ten_category_id\n"
                    + ",b.agent_ten_part_id\n"
                    + ",b.agent_ten_partsku_code\n"
                    + ",a.category_id\n"
                    + ",rel.ten_brand_id\n"
                    + ",rel.ten_part_id\n"
                    + ",rel.ten_partsku_id \n"
                    + "from (select prod_sku_id\n"
                    + ",prod_spu_id,prod_sku_brand_name,ten_partsku_code\n"
                    + ",prod_group_id\n"
                    + ",prod_sku_name\n"
                    + ",prod_sku_code\n"
                    + ",dealer_partsku_id\n"
                    + ",prod_sku_desc\n"
                    + ",prod_sku_desc_img\n"
                    + ",prod_sku_origin_price\n"
                    + ",prod_sku_stock\n"
                    + ",prod_sku_spec\n"
                    + ",prod_sku_weight\n"
                    + ",prod_sku_volume\n"
                    + ",prod_unit_id\n"
                    + ",prod_sku_status\n"
                    + ",prod_sku_min_purchase,ten_sys_id,category_id \n"
                    + "from scps_mysql_ods.t_product_sku final\n"
                    + " where data_flag > 0) a\n"
                    + "  left join\n"
                    + " (select dealer_partsku_id\n"
                    + ",dealer_partsku_code\n"
                    + ",dealer_partsku_external_code\n"
                    + ",dealer_partsku_order_url\n"
                    + ",tenant_id\n"
                    + ",dealer_category_id\n"
                    + ",dealer_part_id\n"
                    + ",agent_tenant_id\n"
                    + ",agent_ten_brand_id\n"
                    + ",agent_ten_category_id\n"
                    + ",agent_ten_part_id\n"
                    + ",agent_ten_partsku_code\n"
                    + ",category_id from scps_mysql_ods.t_dealer_partsku final\n"
                    + "where data_flag > 0) b \n"
                    + "on a.dealer_partsku_id = b.dealer_partsku_id \n"
                    + " left join\n"
                    + "(select prod_sku_id,ten_brand_id,ten_part_id,ten_partsku_id from scps_mysql_ods.t_product_tenant_rel final WHERE data_flag > 0) rel on \n"
                    + "a.prod_sku_id = rel.prod_sku_id",

            "prod_sku_id,prod_group_id,prod_sku_name,dealer_partsku_id,prod_sku_desc,prod_sku_desc_img,prod_sku_origin_price,prod_sku_stock,prod_sku_spec,prod_sku_weight,prod_sku_volume,prod_unit_id,prod_sku_status,prod_sku_min_purchase,dealer_partsku_code,dealer_partsku_external_code,dealer_partsku_order_url,tenant_id,ten_sys_id,dealer_category_id,dealer_part_id,agent_tenant_id,agent_ten_brand_id,agent_ten_category_id,agent_ten_part_id,agent_ten_partsku_code,category_id");

    public static Tuple2<String,String> skuSql = Tuple2.of("SELECT ten_partsku_id,\n" +
                    "ten_partsku_code,\n" +
                    "ten_partsku_external_code,\n" +
                    "ten_partsku_feature,\n" +
                    "ten_partsku_order_url,\n" +
                    "ten_partsku_course_url,\n" +
                    "tenant_id,\n" +
                    "ten_brand_id,\n" +
                    "ten_category_id,\n" +
                    "ten_part_id,\n" +
                    "ten_partsku_desc,\n" +
                    "ten_partsku_desc_img,\n" +
                    "category_id  from scps_mysql_ods.t_tenant_partsku final\n" +
                    "where data_flag > 0",

            "ten_partsku_id,ten_partsku_code,ten_partsku_external_code,ten_partsku_feature,ten_partsku_order_url,ten_partsku_course_url,tenant_id,ten_brand_id,ten_category_id,ten_part_id,ten_partsku_desc,ten_partsku_desc_img,category_id");

    public static Tuple2<String,String> exchangeSql = Tuple2.of("select partsku_exchange_id,\n" +
                    "partsku_exchange_brand_id,\n" +
                    "partsku_exchange_code,\n" +
                    "partsku_exchange_fmt_code,\n" +
                    "partsku_exchange_type,\n" +
                    "tenant_id,\n" +
                    "ten_brand_id,\n" +
                    "ten_category_id,\n" +
                    "ten_part_id,\n" +
                    "ten_partsku_id,\n" +
                    "category_id from scps_mysql_ods.t_tenant_partsku_exchange final\n" +
                    "where data_flag > 0",

            "partsku_exchange_id,partsku_exchange_brand_id,partsku_exchange_code,partsku_exchange_fmt_code,partsku_exchange_type,tenant_id,ten_brand_id,ten_category_id,ten_part_id,ten_partsku_id,category_id");

}

