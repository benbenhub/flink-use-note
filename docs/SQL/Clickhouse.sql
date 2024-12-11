DROP TABLE v1_scps_basics.ods_bosch_sku_local on cluster default_cluster;

TRUNCATE table v1_scps_basics.ods_bosch_sku_local on cluster default_cluster;

TRUNCATE table scps_mysql_ods.t_license_local on cluster default_cluster;

INSERT INTO scps_mysql_ods.t_license
SELECT *,1 FROM mysql('0.0.3.17:3306', 'db', 'dt', 'root', 'ps')


SELECT * FROM scps_mysql_ods.t_tenant_partsku WHERE category_id GLOBAL IN ()


CREATE TABLE IF NOT EXISTS scps_mysql_ods.t_login_account_local ON CLUSTER default_cluster
    ENGINE = ReplacingMergeTree() order by user_id
AS select a.*,b.* from
(SELECT * FROM mysql('0.0.3.17:3306', 'db', 'dt', 'root', 'ps')) a ,
(select toInt8(1) as data_flag) b where 1=2

CREATE TABLE IF NOT EXISTS scps_mysql_ods.t_login_account ON CLUSTER default_cluster
as scps_mysql_ods.t_login_account_local
    ENGINE = Distributed(default_cluster,scps_mysql_ods,t_login_account_local,intHash64(user_id))


INSERT INTO scps_mysql_ods.t_login_account
SELECT *,1 FROM mysql('0.0.3.17:3306', 'db', 'dt', 'root', 'ps')


select * FROM v1_scps_basics.ods_binlog_source WHERE ts_ms > 1733883025606

alter table v1_scps_basics.ads_standard_partsku_recommend_local on CLUSTER dev_ch_cluster add column `std_tree_id` Int64 AFTER category_id;
alter table v1_scps_basics.ads_standard_partsku_recommend on CLUSTER dev_ch_cluster add column `std_tree_id` Int64 AFTER category_id;

alter table v1_scps_basics.dws_standard_partsku_recommend_local on CLUSTER dev_ch_cluster add column `std_tree_id` Int64 AFTER category_id;
alter table v1_scps_basics.dws_standard_partsku_recommend on CLUSTER dev_ch_cluster add column `std_tree_id` Int64 AFTER category_id;


CREATE TABLE scps_mysql_ods.t_area_local on cluster default_cluster
(

    `area_id` Int32,

    `area_name` Nullable(String),

    `city_id` Nullable(Int32),

    `data_flag` Int8
    )
    ENGINE = ReplacingMergeTree
    ORDER BY area_id
    SETTINGS index_granularity = 8192;

CREATE TABLE scps_mysql_ods.t_area on cluster default_cluster
as scps_mysql_ods.t_area_local
    ENGINE = Distributed('default_cluster',
                         'scps_mysql_ods',
                         't_area_local',
                         intHash64(area_id));

CREATE TABLE v1_scps_basics.ods_catcalc_cm_local on cluster default_cluster
(

    `_id` Int64,

    `cm_time` Int64,

    `cm_id` Int64,

    `cm_operational_state` String,

    `cm_history_type` String,

    `cm_origin` String,

    `update_by` String,

    `update_time` DateTime,

    `cm_rel` String,

    `cm_modify_attribute` String
)
    ENGINE = MergeTree
    ORDER BY (_id,
              cm_time)
    SETTINGS index_granularity = 8192;

CREATE TABLE v1_scps_basics.ods_catcalc_cm on cluster default_cluster
as v1_scps_basics.ods_catcalc_cm_local
    ENGINE = Distributed('default_cluster',
                         'v1_scps_basics',
                         'ods_catcalc_cm_local',
                         rand());
