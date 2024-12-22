set 'taskmanager.numberOfTaskSlots' = '3';

--  set 'taskmanager.memory.managed.fraction'='0.2';
set 'taskmanager.memory.process.size' = '4G';

create temporary function JSON_AS_TYPE as 'com.sopei.udf.JsonAsType';

--  create view view_cm as
--  select
--    c.cm_id,
--    c.cm_brand,
--    c.cm_factory,
--    c.cm_model,
--    c.cm_model_year,
--    c.cm_displacement,
--    c.cm_pro['cm_brand_letter'] cm_brand_letter,
--    c.cm_pro['cm_factory_letter'] cm_factory_letter,
--    c.cm_pro['cm_model_letter'] cm_model_letter,
--    c.cm_pro['cm_brakeoil_standard'] cm_brakeoil_standard,
--    c.cm_pro['cm_sales_year'] cm_sales_year,
--    c.cm_pro['cm_stop_year'] cm_stop_year,
--    c.cm_pro['cm_engine_model'] cm_engine_model,
--    c.cm_pro['cm_fuel_type'] cm_fuel_type,
--    c.cm_pro['cm_emission'] cm_emission,
--    c.cm_pro['cm_driving_mode'] cm_driving_mode,
--    c.cm_pro['cm_chassis_model'] cm_chassis_model,
--    c.cm_pro['cm_gearbox'] cm_gearbox,
--    c.cm_pro['cm_max_power'] cm_max_power,
--    c.cm_pro['cm_front_brake_type'] cm_front_brake_type,
--    c.cm_pro['cm_rear_brake_type'] cm_rear_brake_type,
--    c.cm_pro['cm_abs_antilock'] cm_abs_antilock,
--    c.cm_pro['cm_conf_level'] cm_conf_level,
--    c.cm_pro['cm_coolant_standard'] cm_coolant_standard,
--    c.cm_pro['cm_gearbox_model'] cm_gearbox_model,
--    c.cm_pro['cm_block_num'] cm_block_num,
--    c.cm_pro['cm_trans_type'] cm_trans_type,
--    c.cm_pro['cm_driving_type'] cm_driving_type,
--    c.cm_pro['cm_park_brake_type'] cm_park_brake_type,
--    c.cm_pro['cm_gearbox_drywet'] cm_gearbox_drywet,
--    c.cm_pro['cm_front_wheel'] cm_front_wheel,
--    c.cm_pro['cm_rear_wheel'] cm_rear_wheel,
--    c.cm_pro['cm_support_type'] cm_support_type,
--    c.cm_pro['cm_engine_start_stop'] cm_engine_start_stop,
--    c.cm_pro['cm_adaptive_cruise_control'] cm_adaptive_cruise_control,
--    c.cm_pro['cm_automatic_parking'] cm_automatic_parking,
--    c.cm_pro['cm_airrefrigerant_standard'] cm_airrefrigerant_standard,
--    c.cm_pro['cm_front_suspension_type'] cm_front_suspension_type,
--    c.cm_pro['cm_rear_suspension_type'] cm_rear_suspension_type,
--    c.cm_pro['cm_spark_plug_num'] cm_spark_plug_num,
--    c.cm_pro['cm_steeroil_standard'] cm_steeroil_standard,
--    c.cm_pro['cm_model_price'] cm_model_price
--  from
--    (
--      select
--        cm_id,
--        cm_brand,
--        cm_factory,
--        cm_model,
--        cm_model_year,
--        cm_displacement,
--        JSON_AS_TYPE (cm_carmodel) cm_pro
--      from
--        clickhouse.v1_scps_carmodel.ods_t_carmodel_base_rt
--      WHERE
--        data_flag > 0
--    ) c;

-- Flink SQL 在 Left Join 时右表不要写 Where 语句，需要 Where 时用子查询，不然会得到 Inner Join 的结果
-- 根本原因是 Left Join 右表数据为 NULL
--  create view view_cmskuid as
--  select
--    c.*,
--    r.ten_cm_rel_id,
--    r.std_tree_id,
--    r.std_partsku_id,
--    r.oem_partsku_id,
--    r.ten_partsku_id,
--    r.oem_partsku_fmt_code,
--    r.ten_cm_rel_flag,
--    r.ten_cm_comment
--  from
--    view_cm c
--    LEFT JOIN (SELECT * FROM clickhouse.scps_mysql_ods.t_tenant_cm_rel WHERE data_flag > 0) r on r.cm_id = c.cm_id
--  ;

create view view_cmsku as
select
    r.*,
    s.ten_part_id,
    s.ten_partsku_code,
    s.ten_partsku_name,
    s.ten_partsku_unicode,
    s.ten_partsku_type,
    s.ten_partsku_down_switch,
    s.ten_partsku_production_status,
    s.ten_partsku_external_code,
    s.ten_partsku_short_code,
    s.ten_partsku_feature,
    s.ten_partsku_order_url,
    s.ten_partsku_course_url,
    s.ten_partsku_wechat_channels_name,
    s.ten_partsku_wechat_channels_feedid,
    s.ten_partsku_desc,
    s.ten_partsku_desc_img,
    s.ten_partsku_id as sku_id,
    s.tenant_id,
    s.ten_brand_id,
    s.ten_category_id,
    s.category_id
from
    (select ten_partsku_id,ten_partsku_code,ten_partsku_name,ten_partsku_unicode,ten_partsku_type,ten_partsku_down_switch,ten_partsku_production_status,ten_partsku_external_code,ten_partsku_short_code,ten_partsku_feature,ten_partsku_order_url,ten_partsku_course_url,ten_partsku_wechat_channels_name,ten_partsku_wechat_channels_feedid,tenant_id,ten_brand_id,ten_category_id,ten_part_id,ten_partsku_desc,ten_partsku_desc_img,category_id,ten_data_type,data_flag FROM clickhouse.scps_mysql_ods.t_tenant_partsku WHERE
        -- category_id IN (SELECT category_id FROM clickhouse.scps_mysql_ods.t_category WHERE category_new_type = 'LICENSE') AND
        data_flag > 0
     union ALL select 0,'','','','','','','','','','','','','',0,0,0,0,'','',0,'',1
    ) s
        LEFT JOIN clickhouse.v1_scps_bi.dwd_bus_cm r on r.ten_partsku_id = s.ten_partsku_id
;

-- 所有产品关联所有关系
-- Caused by: org.apache.flink.table.api.TableException: group count must be less than 64.
insert into
    clickhouse.v1_scps_bi.dwd_bus_cmskurel
select
    *
FROM
    view_cmsku