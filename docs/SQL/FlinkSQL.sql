create database if not exists paimon.bi_statistics;

-- MySQL 业务表
DROP TABLE IF EXISTS paimon.bi_statistics.dwd_wg_access;
CREATE TABLE IF NOT EXISTS paimon.bi_statistics.dwd_wg_access (
    log_time TIMESTAMP(3) NOT NULL  COMMENT '请求创建时间(yyyy-MM-dd hh:mm:ss.000)',
    log_date DATE COMMENT '请求创建日期(yyyy-MM-dd)',
    trace_id STRING,
    host STRING,
    remote_addr STRING,
    url STRING,
    status STRING,
    referer STRING,
    user_agent STRING,
    response_time STRING,
    api_type_code STRING,
    ten_sys_flag STRING,
    ten_sys_flag_format STRING,
    user_id STRING,
    tenant_id BIGINT,
    ten_app_id BIGINT,
    WATERMARK FOR log_time AS log_time - INTERVAL '60' SECOND
    )  PARTITIONED BY (log_date) WITH (
                                     'write-mode' = 'append-only',
                                     'write-only' = 'false',
                                     'consumer.expiration-time' = '48 h',
                                     -- 快照相关 保留时间
                                     'snapshot.time-retained' = '4h',
                                     'snapshot.num-retained.min' = '10',
                                     -- 'snapshot.num-retained.max' = '500',
                                     -- 同步过期快照，会阻塞flink
                                     'snapshot.expire.execution-mode' = 'sync',
                                     -- 同时最多过期快照数量
                                     'snapshot.expire.limit' = '100',
                                     -- ==============tag相关，使用watermark作为时间基准=======================
                                     'tag.automatic-creation' = 'watermark',
                                     -- 每天创建一次TAG
                                     'tag.creation-period' = 'daily',
                                     -- TAG保留时间，半年
                                     'tag.num-retained-max' = '90',
                                     -- 延迟1小时创建TAG
                                     'tag.creation-delay' = '1 h',
                                     -- ====================分区相关===============================
                                     --分区过期检查时间
                                     'partition.expiration-check-interval' = '12h',
                                     -- 分区过期时间
                                     'partition.expiration-time' = '90d',
                                     -- 格式化相关
                                     'partition.timestamp-formatter' = 'yyyy-MM-dd',
                                     'partition.timestamp-pattern' = '$log_date'
                                     );
-- 日志表 接口访问表
DROP TABLE IF EXISTS paimon.bi_statistics.dwd_interface_access;
CREATE TABLE IF NOT EXISTS paimon.bi_statistics.dwd_interface_access (
    log_time TIMESTAMP(3) NOT NULL  COMMENT '请求创建时间(yyyy-MM-dd hh:mm:ss.000)',
    log_date DATE COMMENT '请求创建日期(yyyy-MM-dd)',
    `hour` BIGINT,
    region_id INT,
    region_name STRING,
    province_id INT,
    province_name STRING,
    city_id INT,
    city_name STRING,
    host STRING,
    remote_addr STRING,
    `module` STRING,
    user_id STRING,
    ten_sys_flag STRING,
    ten_sys_flag_format STRING,
    tenant_id BIGINT,
    ten_app_id BIGINT,
    trace_id STRING,
    server_url STRING,
    params STRING,
    results STRING,
    --  log_data STRING,
    WATERMARK FOR log_time AS log_time - INTERVAL '60' SECOND
    )  PARTITIONED BY (log_date) WITH (
                                     'write-mode' = 'append-only',
                                     'write-only' = 'false',
                                     'consumer.expiration-time' = '48 h',
                                     -- 快照相关 保留时间
                                     'snapshot.time-retained' = '4h',
                                     'snapshot.num-retained.min' = '10',
                                     -- 'snapshot.num-retained.max' = '500',
                                     -- 同步过期快照，会阻塞flink
                                     'snapshot.expire.execution-mode' = 'sync',
                                     -- 同时最多过期快照数量
                                     'snapshot.expire.limit' = '100',
                                     -- ==============tag相关，使用watermark作为时间基准=======================
                                     'tag.automatic-creation' = 'watermark',
                                     -- 每天创建一次TAG
                                     'tag.creation-period' = 'daily',
                                     -- TAG保留时间，半年
                                     'tag.num-retained-max' = '90',
                                     -- 延迟1小时创建TAG
                                     'tag.creation-delay' = '1 h',
                                     -- ====================分区相关===============================
                                     --分区过期检查时间
                                     'partition.expiration-check-interval' = '12h',
                                     -- 分区过期时间
                                     'partition.expiration-time' = '90d',
                                     -- 格式化相关
                                     'partition.timestamp-formatter' = 'yyyy-MM-dd',
                                     'partition.timestamp-pattern' = '$log_date'
                                     );


set 'taskmanager.numberOfTaskSlots' = '4';
--  set 'taskmanager.memory.managed.fraction'='0.2';
set 'taskmanager.memory.process.size' = '3G';

--  SET pipeline.operator-chaining=false;
SET 'execution.checkpointing.interval' = '2m';

SET 'execution.checkpointing.timeout' = '10m';

SET 'execution.checkpointing.min-pause' = '1m';

SET 'execution.checkpointing.unaligned.enabled' = 'false';

SET 'paimon.paimon.log_warehouse.dws_logs.scan.parallelism' = '4';


--  SET 'paimon.paimon.log_warehouse.dws_logs.scan.timestamp' = '1725744215000';
--  SET 'paimon.paimon.log_warehouse.dws_logs.scan.timestamp-millis' = 1725657815000;
--  SET 'paimon.paimon.bi_statistics.dwd_interface_access.sink.parallelism' = '2';

SET 'paimon.paimon.log_warehouse.dws_logs.scan.mode' = 'latest';
SET 'paimon.paimon.log_warehouse.log_gateway_record.scan.mode' = 'latest';
SET 'paimon.paimon.bi_statistics.dwd_interface_access.scan.mode' = 'latest';
SET 'paimon.paimon.bi_statistics.dwd_query_sku.scan.mode' = 'latest';

--  ADD JAR 'rs:/lib/scps_udf_acc.jar';
create temporary function GROUP_STRING_ARRAY as 'com.sopei.udf.acc.GroupStringArray';

create temporary function ARRAY_FILTER as 'com.sopei.udf.arr.ArrayFilter';

create temporary function split_toarray as 'com.sopei.udf.SplitToArray';

create temporary function TSF_FORMAT as 'com.sopei.udf.bi.TenSysFlagFormat';

create temporary function JSON_AS_TYPE as 'com.sopei.udf.JsonAsType';

create temporary function WECOM_RESULT as 'com.sopei.udf.bi.WecomResult';
create temporary function WECOM_RESULT2 as 'com.sopei.udf.bi.WecomResultRes';

create temporary function ENGINE_OIL_RESULT as 'com.sopei.udf.bi.EngineOilResult';

create temporary function PARTSKU_CM_RESULT as 'com.sopei.udf.bi.PartskuCmResult';

-- 接口日志聚合
create view view_dwd_interface_access as
select
    az.log_time,
    az.log_date,
        HOUR(az.log_time) as `hour`,
        r.region_id,
        r.region_name,
        b.province_id,
        b.province_name,
        c.city_id,
        c.city_name,
        az.host,
        az.remote_addr,
        az.moudle,
        az.user_id,
        az.ten_sys_flag,
        az.ten_sys_flag_format,
        tb.tenant_id,
        tb.ten_app_id,
        az.trace_id,
        az.server_url,
        IF(az.moudle = 'api_wecom', ARRAY_FILTER(az.log_data, '.*"biz_log_type":"RESPONSE".*')[1], ARRAY_FILTER(az.log_data, '.*"biz_log_type":"REQUEST".*')[1]) as p,
        IF(az.moudle = 'api_wecom', ARRAY_FILTER(az.log_data, '.* RESPONSE .*')[1], ARRAY_FILTER (az.log_data, '.*"biz_log_type":"RESPONSE".*') [1]) as r
        FROM
        (
        SELECT DISTINCT
        window_start,
        window_end,
        PROCTIME () AS proctime,
        MAX(log_time) as log_time,
        log_date,
        host,
        remote_addr,
        moudle,
        user_id,
        ten_sys_flag,
        TSF_FORMAT (ten_sys_flag, host) as ten_sys_flag_format,
        trace_id,
        server_url,
        GROUP_STRING_ARRAY (DISTINCT `data`) as log_data,
        provience,
        city
        FROM
        TABLE (
        TUMBLE (
        TABLE paimon.log_warehouse.dws_logs,
        DESCRIPTOR (log_time),
        INTERVAL '5' MINUTES
        )
        )
        WHERE
        logtype = 'biz'
    -- AND REGEXP (data, '.*bi-use-.*')
        AND ((moudle <> 'api_wecom' AND `data` IS JSON OBJECT AND JSON_EXISTS(`data`, '$.biz_log_type')) OR (moudle = 'api_wecom' AND REGEXP(`data`,'.*RESPONSE.*')))
        AND host IN (
        'www.sosoqipei.com',
        'soso.sopei.cn',
        'weapp.sopei.cn',
        'u.sopei.cn',
        'api.sopei.cn',
        'embed.sopei.cn',
        'callback.sopei.cn',
        'wx.sopei.cn'
        )
        GROUP BY
        window_start,
        window_end,
        GROUPING SETS (
        (
        log_date,
        host,
        remote_addr,
        moudle,
        user_id,
        ten_sys_flag,
        trace_id,
        server_url,
        provience,
        city
        )
        )
        ) as az
        LEFT JOIN clickhouse.scps_mysql_ods.t_province FOR SYSTEM_TIME AS OF az.proctime AS b on b.province_name = az.provience
        LEFT JOIN clickhouse.scps_mysql_ods.t_city FOR SYSTEM_TIME AS OF az.proctime AS c on c.city_name = az.city
        LEFT JOIN clickhouse.scps_mysql_ods.t_region FOR SYSTEM_TIME AS OF az.proctime AS r on r.region_id = b.region_id
        LEFT JOIN clickhouse.scps_mysql_ods.t_tenant_app FOR SYSTEM_TIME AS OF az.proctime AS tb on tb.ten_app_code = az.ten_sys_flag_format;

insert into
    paimon.bi_statistics.dwd_interface_access (
    log_time,
    log_date,
    `hour`,
    region_id,
    region_name,
    province_id,
    province_name,
    city_id,
    city_name,
    host,
    remote_addr,
    `module`,
    user_id,
    ten_sys_flag,
    ten_sys_flag_format,
    tenant_id,
    ten_app_id,
    trace_id,
    server_url,
    params,
    results
)
select
    log_time,
    log_date,
    `hour`,
    IFNULL (region_id, 0),
    IFNULL (region_name, ''),
    IFNULL (province_id, 0),
    IFNULL (province_name, ''),
    IFNULL (city_id, 0),
    IFNULL (city_name, ''),
    host,
    remote_addr,
    moudle,
    user_id,
    ten_sys_flag,
    ten_sys_flag_format,
    tenant_id,
    ten_app_id,
    trace_id,
    server_url,
    IFNULL (p, ''),
    IFNULL (r, '')
FROM
    view_dwd_interface_access;

INSERT INTO paimon.bi_statistics.dwd_wg_access
SELECT
    wg.log_time,
    wg.log_date,
    wg.trace_id,
    wg.host,
    wg.remote_addr,
    wg.url,
    wg.status,
    wg.referer,
    wg.user_agent,
    wg.response_time,
    wg.api_type_code,
    wg.ten_sys_flag,
    wg.ten_sys_flag_format,
    wg.user_id,
    tb.tenant_id,
    tb.ten_app_id
FROM
    (SELECT
         *,
         PROCTIME () AS proctime,
         TSF_FORMAT (ten_sys_flag, host) as ten_sys_flag_format
     FROM paimon.log_warehouse.log_gateway_record WHERE api_type_code IS NOT null AND ten_sys_flag <> '') as wg
        LEFT JOIN clickhouse.scps_mysql_ods.t_tenant_app FOR SYSTEM_TIME AS OF wg.proctime AS tb on tb.ten_app_code = wg.ten_sys_flag_format;


-- 查询产品接口参数结果类型转换
create view view_dwd_query_sku as
SELECT
    tw.log_time,
    tw.log_date,
    tw.`hour`,
    tw.region_id,
    tw.region_name,
    tw.province_id,
    tw.province_name,
    tw.city_id,
    tw.city_name,
    tw.host,
    tw.remote_addr,
    tw.`module`,
    tw.user_id,
    tw.ten_sys_flag,
    tw.ten_sys_flag_format,
    tw.tenant_id,
    tw.ten_app_id,
    tw.trace_id,
    tw.server_url,
    tw.p,
    --  MAP['1','2'] as r
    --  ARRAY[''] as r
    tw.r,
    CASE
        WHEN tw.server_url IN (
                               'POST-/api/weapp/v3.0/code/skus/list',
                               'POST-/api/web/v3.0/code/skus/list',
                               'GET-/api/web/v2.0/dealer/partskus/exchange',
                               'GET-/api/web/v2.0/dealer/partskus/tenant',
                               'GET-/api/web/v2.0/dealer/partskus/oem',
                               'GET-/api/web/v2.0/dealer/partskus/fms',
                               'GET-/api/web/v2.0/dealer/partskus/indus',
                               'GET-/api/web/v2.0/dealer/partskus/gearbox/code',
                               'GET-/api/weapp/v2.0/dealer/partskus/exchange',
                               'GET-/api/weapp/v2.0/dealer/partskus/tenant',
                               'GET-/api/weapp/v2.0/dealer/partskus/oem',
                               'GET-/api/weapp/v2.0/dealer/partskus/fms',
                               'GET-/api/weapp/v2.0/dealer/partskus/indus',
                               'GET-/api/weapp/v2.0/dealer/partskus/gearbox/code'
            ) THEN 'CODE_QUERY'
        WHEN tw.server_url IN (
                               'GET-/api/web/v2.0/dealer/partskus/carmodel',
                               'GET-/api/web/v2.0/dealer/wiper/partsku',
                               'GET-/api/weapp/v2.0/dealer/partskus/carmodel',
                               'GET-/api/weapp/v2.0/dealer/wiper/partsku'
            ) THEN 'CARMODEL_QUERY'
        WHEN tw.server_url IN (
                               'GET-/api/web/v2.0/dealer/partskus/cm',
                               'GET-/api/web/v2.0/dealer/engineoil/partskus/carmodel',
                               'GET-/api/weapp/v2.0/dealer/partskus/cm',
                               'GET-/api/weapp/v2.1/dealer/engineoil/partskus/carmodel',
                               'GET-/api/weapp/v2.2/dealer/engineoil/partskus/carmodel'
            ) AND tw.p['vin_code'] IS NOT NULL
            THEN 'VIN_QUERY'
        WHEN tw.server_url IN (
                               'GET-/api/web/v2.0/dealer/partskus/cm',
                               'GET-/api/web/v2.0/dealer/engineoil/partskus/carmodel',
                               'GET-/api/weapp/v2.0/dealer/partskus/cm',
                               'GET-/api/weapp/v2.1/dealer/engineoil/partskus/carmodel',
                               'GET-/api/weapp/v2.2/dealer/engineoil/partskus/carmodel'
            ) AND tw.p['vin_code'] IS NULL
            THEN 'CARMODEL_QUERY'
        WHEN tw.server_url IN ('GET-/api/web/v2.0/dealer/tyre/partskus/size','GET-/api/weapp/v2.0/dealer/tyre/partskus/size')
            THEN 'TYRE_SIZE_QUERY'
        WHEN tw.server_url IN ('GET-/api/web/v2.0/dealer/engineoil/partskus/engine',
                               'GET-/api/weapp/v2.1/dealer/engineoil/partskus/engine',
                               'GET-/api/weapp/v2.2/dealer/engineoil/partskus/engine')
            THEN 'ENGINE_MODEL_QUERY'
        ELSE IFNULL(tw.p['query_mode'], '')
        END AS query_mode
FROM
    (select *,JSON_AS_TYPE(params) as p,
            CASE
                WHEN server_url IN (
                                    'GET-/api/web/v2.0/dealer/engineoil/partskus/carmodel',
                                    'GET-/api/web/v2.0/dealer/engineoil/partskus/engine',
                                    'GET-/api/weapp/v2.1/dealer/engineoil/partskus/carmodel',
                                    'GET-/api/weapp/v2.2/dealer/engineoil/partskus/carmodel',
                                    'GET-/api/weapp/v2.1/dealer/engineoil/partskus/engine',
                                    'GET-/api/weapp/v2.2/dealer/engineoil/partskus/engine')
                    THEN ENGINE_OIL_RESULT(results)
                WHEN server_url IN ('GET-/api/web/v2.0/dealer/partskus/cm','GET-/api/weapp/v2.0/dealer/partskus/cm')
                    THEN PARTSKU_CM_RESULT(results)
                WHEN server_url IN ('GET-/api/web/v2.0/dealer/tyre/partskus/size',
                                    'GET-/api/weapp/v2.0/dealer/tyre/partskus/size')
                    THEN JSON_AS_TYPE(JSON_AS_TYPE(IFNULL(JSON_AS_TYPE(results)['data'], ''))['tyres'], 'ARRAY')
                WHEN server_url IN ('GET-/api/web/v2.0/dealer/wiper/partsku','GET-/api/weapp/v2.0/dealer/wiper/partsku')
                    THEN ARRAY[JSON_AS_TYPE(results)['data']]
                ELSE JSON_AS_TYPE(IFNULL(JSON_AS_TYPE(results)['data'], ''), 'ARRAY')
                END as r FROM paimon.bi_statistics.dwd_interface_access WHERE server_url IN (
        -- u.sopei.cn bff_web
                                                                                             'POST-/api/web/v3.0/carmodel/skus/list',
                                                                                             'POST-/api/web/v3.0/code/skus/list',
                                                                                             'GET-/api/web/v2.0/dealer/partskus/carmodel',
                                                                                             'GET-/api/web/v2.0/dealer/partskus/exchange',
                                                                                             'GET-/api/web/v2.0/dealer/partskus/tenant',
                                                                                             'GET-/api/web/v2.0/dealer/partskus/oem',
                                                                                             'GET-/api/web/v2.0/dealer/partskus/fms',
                                                                                             'GET-/api/web/v2.0/dealer/partskus/indus',
                                                                                             'GET-/api/web/v2.0/dealer/partskus/cm',
                                                                                             'GET-/api/web/v2.0/dealer/engineoil/partskus/engine',
                                                                                             'GET-/api/web/v2.0/dealer/engineoil/partskus/carmodel',
                                                                                             'GET-/api/web/v2.0/dealer/partskus/gearbox/code',
                                                                                             'GET-/api/web/v2.0/dealer/partskus/gearbox/carmodel',
                                                                                             'GET-/api/web/v2.0/dealer/gearboxfilter/partskus/gearbox',
                                                                                             'GET-/api/web/v2.0/dealer/gearboxfilter/partskus/carmodel',
                                                                                             'GET-/api/web/v2.0/dealer/tyre/partskus/size',
                                                                                             'GET-/api/web/v2.0/dealer/wiper/partsku',
        -- weapp.sopei.cn weapp
                                                                                             'POST-/api/weapp/v3.0/carmodel/skus/list',
                                                                                             'POST-/api/weapp/v3.0/code/skus/list',
                                                                                             'GET-/api/weapp/v2.0/dealer/partskus/carmodel',
                                                                                             'GET-/api/weapp/v2.0/dealer/partskus/exchange',
                                                                                             'GET-/api/weapp/v2.0/dealer/partskus/tenant',
                                                                                             'GET-/api/weapp/v2.0/dealer/partskus/oem',
                                                                                             'GET-/api/weapp/v2.0/dealer/partskus/fms',
                                                                                             'GET-/api/weapp/v2.0/dealer/partskus/indus',
                                                                                             'GET-/api/weapp/v2.0/dealer/partskus/cm',
                                                                                             'GET-/api/weapp/v2.1/dealer/engineoil/partskus/engine',
                                                                                             'GET-/api/weapp/v2.1/dealer/engineoil/partskus/carmodel',
                                                                                             'GET-/api/weapp/v2.2/dealer/engineoil/partskus/engine',
                                                                                             'GET-/api/weapp/v2.2/dealer/engineoil/partskus/carmodel',
                                                                                             'GET-/api/weapp/v2.0/dealer/partskus/gearbox/code',
                                                                                             'GET-/api/weapp/v2.0/dealer/partskus/gearbox/carmodel',
                                                                                             'GET-/api/weapp/v2.0/dealer/gearboxfilter/partskus/gearbox',
                                                                                             'GET-/api/weapp/v2.0/dealer/gearboxfilter/partskus/carmodel',
                                                                                             'GET-/api/weapp/v2.0/dealer/tyre/partskus/size',
                                                                                             'GET-/api/weapp/v2.0/dealer/wiper/partsku')) as tw
UNION ALL
SELECT
    tc.log_time,
    tc.log_date,
    tc.`hour`,
    tc.region_id,
    tc.region_name,
    tc.province_id,
    tc.province_name,
    tc.city_id,
    tc.city_name,
    tc.host,
    tc.remote_addr,
    tc.`module`,
    tc.user_id,
    tc.ten_sys_flag,
    tc.ten_sys_flag_format,
    tc.tenant_id,
    tc.ten_app_id,
    tc.trace_id,
    tc.server_url,
    JSON_AS_TYPE(IFNULL(tc.cr[1], '')) as p,
    --  JSON_AS_TYPE(IFNULL(tc.cr[2], ''), 'ARRAY') as r,
    --  JSON_AS_TYPE(IFNULL(tc.rr, ''), 'ARRAY') as r,
    tc.rr,
    'VIN_QUERY' as query_mode
FROM (SELECT *,WECOM_RESULT(params) as cr,WECOM_RESULT2(results) as rr FROM paimon.bi_statistics.dwd_interface_access WHERE
                                                                                                                        -- callback.sopei.cn
        `module` = 'api_wecom' AND server_url = 'POST-/api/wecom/v2.0/wecom/session' AND results <> '') as tc
;

insert into
    paimon.bi_statistics.dwd_query_sku
select
    log_time,
    log_date,
    `hour`,
    region_id,
    region_name,
    province_id,
    province_name,
    city_id,
    city_name,
    host,
    remote_addr,
    `module`,
    user_id,
    ten_sys_flag,
    ten_sys_flag_format,
    tenant_id,
    ten_app_id,
    trace_id,
    server_url,
    p,
    r,
    query_mode
FROM
    view_dwd_query_sku;


-- sku 展开
create view view_dwd_sku as
SELECT
    log_time,
    log_date,
    `hour`,
    region_id,
    region_name,
    province_id,
    province_name,
    city_id,
    city_name,
    host,
    remote_addr,
    `module`,
    user_id,
    ten_sys_flag_format,
    tenant_id,
    ten_app_id,
    trace_id,
    server_url,
    params,
    JSON_AS_TYPE (sku_msg) sku_msg_map,
    query_mode
FROM
    paimon.bi_statistics.dwd_query_sku
        CROSS JOIN UNNEST (results) AS t (sku_msg);

insert into
    paimon.bi_statistics.dwd_sku_expand
select
    log_time,
    log_date,
    `hour`,
    region_id,
    region_name,
    province_id,
    province_name,
    city_id,
    city_name,
    host,
    remote_addr,
    `module`,
    user_id,
    ten_sys_flag_format,
    tenant_id,
    ten_app_id,
    trace_id,
    server_url,
    params,
    sku_msg_map,
    query_mode,
    sku_msg_map['ten_partsku_id'],
    sku_msg_map['ten_partsku_code'],
    sku_msg_map['prod_sku_id'],
    sku_msg_map['prod_sku_code'],
    sku_msg_map['brand_id'],
    sku_msg_map['brand_name'],
    sku_msg_map['category_code'],
    sku_msg_map['category_name']
FROM
    view_dwd_sku WHERE `module` <> 'api_wecom'
UNION ALL
SELECT
    a.log_time,
    a.log_date,
    a.`hour`,
    a.region_id,
    a.region_name,
    a.province_id,
    a.province_name,
    a.city_id,
    a.city_name,
    a.host,
    a.remote_addr,
    a.`module`,
    a.user_id,
    a.ten_sys_flag_format,
    b.tenant_id,
    --  0 as tenant_id,
    IFNULL(a.ten_app_id,0),
    a.trace_id,
    a.server_url,
    a.params,
    a.sku_msg_map,
    a.query_mode,
    CAST(a.bn AS STRING),
    a.sku_msg_map['ten_partsku_code'],
    --  b.ten_partsku_code,
    a.sku_msg_map['prod_sku_id'],
    a.sku_msg_map['prod_sku_code'],
    '' as brand_id,
    a.sku_msg_map['brand_name'],
    a.sku_msg_map['category_code'],
    a.sku_msg_map['category_name']
FROM
    (select
         *,PROCTIME () AS proctime,CAST(IFNULL(sku_msg_map['partsku_id'], '0') AS BIGINT) as bn
         --sku_msg_map['partsku_id'] as bn
     FROM
         view_dwd_sku WHERE `module` = 'api_wecom') a
        LEFT JOIN clickhouse.scps_mysql_ods.t_tenant_partsku FOR SYSTEM_TIME AS OF a.proctime AS b on b.ten_partsku_id = a.bn
;

--  ADD CUSTOMJAR  'rs:/cdc/flink-cdc-dist-3.1.0.jar';
--  ADD CUSTOMJAR  'rs:/cdc/flink-cdc-pipeline-connector-doris-3.1.0.jar';
--  ADD CUSTOMJAR  'rs:/cdc/flink-cdc-pipeline-connector-mysql-3.1.0.jar';
SET 'taskmanager.memory.process.size' = '3G';
SET 'jobmanager.memory.process.size' = '800m';

SET 'execution.checkpointing.interval' = '5m';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '3';
EXECUTE PIPELINE WITHYAML (
source:
  type: mysql
  hostname: 0.1.4.0
  port: 35423
  username: release
  password: 'ps'
  --  tables: v2_sp_tenant_partsku.\.*,v2_sp_standard_partsku.\.*,v2_sp_carmodel.\.*,v2_sp_oem_partsku.\.*,v2_sp_industry_partsku.\.*,v2_sp_famous_partsku.\.*,v2_sp_part_category.\.*
  tables: v2_sp_carmodel.t_std_tree_carmodel_rel_[0-9]+,v2_sp_carmodel.t_carmodel_tyre
  server-id: 6499-6599

sink:
  type: paimon
  catalog.properties.matastore: filesystem
  catalog.properties.warehouse: 's3://xxx/'
  catalog.properties.s3.path.style.access: 'true'
  catalog.properties.s3.endpoint: 'xxx.com'
  catalog.properties.s3.access-key: 'ak'
  catalog.properties.s3.secret-key: 'sk'
  --  catalog.properties.uri: 'jdbc:mysql://0.1.4.0:35423/paimon'
  --  catalog.properties.jdbc.user: 'u'
  --  catalog.properties.jdbc.password: 'p'
  --  catalog.properties.catalog-key: 'mysql-jdbc'
  --  catalog.properties.metastore: 'jdbc'
  --  catalog.properties.lock.enabled: 'true'
  table.properties.bucket: 1

--  transform:
--    - source-table: v2_sp_tenant_partsku.t_oem_carmodel_rel_[0-9]+
--      projection: '\*'
--      partition-keys: std_tree_id
--    - source-table: v2_sp_carmodel.t_std_tree_carmodel_[0-9]+
--      projection: '\*'
--      partition-keys: std_tree_cm_conf_id
--    - source-table: v2_sp_carmodel.t_std_tree_carmodel_rel_[0-9]+
--      projection: '\*'
--      partition-keys: std_tree_cm_conf_id
--    - source-table: v2_sp_carmodel.t_tenant_cm_rel_[0-9]+_[0-9]+
--      projection: '\*'
--      partition-keys: std_tree_id
--    - source-table: v2_sp_carmodel.t_std_tree_carmodel_conf
--      projection: \*
--      table-options: bucket=1
--    - source-table: v2_sp_carmodel.t_carmodel_gear
--      projection: \*
--      table-options: bucket=1

route:
  - source-table: v2_sp_carmodel.t_std_tree_carmodel_conf
    sink-table: scps_mysql_ods.t_std_tree_carmodel_conf
  - source-table: v2_sp_carmodel.t_std_tree_carmodel_conf_rel
    sink-table: scps_mysql_ods.t_std_tree_carmodel_conf_rel


pipeline:
  name: MySQL to Paimon Pipeline
  parallelism: 1
)

