
SHOW RESOURCES

SHOW BROKER;

create database if not exists scps_bi;

SHOW CATALOGS

SHOW CREATE CATALOG	paimon;
SHOW CREATE CATALOG	clickhouse_online;

SHOW CREATE table paimon.log_warehouse.dws_logs;


CREATE CATALOG `paimon` PROPERTIES (
"warehouse" = "cosn://flink-paimon-1251517753/",
"use_meta_cache" = "true",
"type" = "paimon",
"create_time" = "2024-08-19 10:02:39.260",
"cos.secret_key" = "sk",
"cos.endpoint" = "xx.com",
"cos.access_key" = "ak"
);

DROP CATALOG ch_online;

CREATE CATALOG `clickhouse_online` PROPERTIES (
"user" = "root",
"use_meta_cache" = "false",
"type" = "jdbc",
"password" = "ps",
"jdbc_url" = "jdbc:clickhouse://0.0.0.0:8123/db",
"driver_url" = "clickhouse-jdbc-0.4.6.jar",
"driver_class" = "com.clickhouse.jdbc.ClickHouseDriver"
);

DROP CATALOG paimon;

SWITCH paimon;

SHOW DATABASES;

use bi_statistics;

show tables;

CREATE TABLE IF NOT EXISTS scps_bi.ads_vin_query
(
    `tenant_id` BIGINT NOT NULL,
    `ten_app_id` BIGINT NOT NULL,
    `ten_brand_id` BIGINT NOT NULL,
    `ten_category_id` BIGINT NOT NULL,
    `ten_app_code` VARCHAR(20) NOT NULL,
    `bran_name` VARCHAR(20) DEFAULT '',
    `brand_category_name` VARCHAR(20) DEFAULT '',
    -- `vin_code` VARCHAR(20) NOT NULL COMMENT "vin",
    `calc_date` DATE NOT NULL COMMENT "日期每天",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:0	0:00" COMMENT "最后一次查询时间",
    `query_count` BIGINT SUM DEFAULT "0" COMMENT "vin总查询次数"
    )
    AGGREGATE KEY(`tenant_id`, `ten_app_id`,`ten_brand_id`,`ten_category_id`,
                  `ten_app_code`, `bran_name`, `brand_category_name`, `calc_date`)
    DISTRIBUTED BY HASH(`ten_category_id`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
               );



DROP TABLE scps_bi.t_ten_statistical

TRUNCATE table scps_bi.t_ten_statistical

select * FROM paimon.bi_statistics.dwd_query_sku WHERE log_date = '2024-12-03'

show create table scps_bi.t_ten_statistical


SELECT ten_partsku_code,category_name FROM scps_bi.t_sku_query
    INTO OUTFILE "s3://flink-checkpoint-1251517753/flink_output_file/doris"
FORMAT AS CSV
PROPERTIES
(
    "AWS_ENDPOINT" = "xxx.com",
    "AWS_ACCESS_KEY" = "ak",
    "AWS_SECRET_KEY" = "sk",
    "AWS_REGION" = "xxx,
    "column_separator" = ",",
    "line_delimiter" = "\n",
    "max_file_size" = "1024MB"
);


