import logging
from clickhouse_driver import Client
from datetime import datetime, timedelta
import pandas as pd
import sys
import os
import re
import requests
sys.path.append(os.getenv('RES_PATH') + '/password')
from platform_function import *

source_password()

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(message)s")

csv_path = '/apps/ods-mysql-clickhouse.txt'
mysql_url = os.getenv('MYSQL_READ_URL')
mysql_un = os.getenv('MYSQL_READ_USERNAME')
mysql_ps = os.getenv('MYSQL_READ_PASSWORD')

ch_database = 'scps_mysql_ods'
ch_cluster = 'default_cluster'
ch_host = "0.0.0.0"
ch_port = "9000"
ch_user = os.getenv('CLICKHOUSE_USERNAME')
ch_password = os.getenv('CLICKHOUSE_PASSWORD')
# 超时时间send_receive_timeout = 25  #
ch_client = Client(host=ch_host, port=ch_port, user=ch_user, password=ch_password, database=ch_database,
                send_receive_timeout=25)

def ods_ch_start(pa_kf):
    print(f"kafka offset: {pa_kf}")
    headers = {
        'Content-Type': 'application/json'
    }
    p_body = {
        'id': 123,
        'variables': {'d_ods_op_offset': pa_kf}
    }
    rel_job = requests.post("http://10.43.54.183:8888/openapi/submitTask", json=p_body, headers=headers)
    if rel_job.status_code == 200:
        print('增量程序提交成功。。。')
    else:
        raise SystemError('增量程序提交失败:{}'.format(rel_job.text))

def split_table_aggregation(tb_multi, ch_tb_name, ms_db_name, order_id):
    """MySQL分表 数据同步
        :param tb_multi: clickhouse正则函数查询所有分表
        :param ch_tb_name: clickhouse的表
        :param ms_db_name: MySQL的数据库
        :param order_id: clickhouse本地表的order_id
    """

    tb_list_sql = f"SELECT TABLE_NAME FROM mysql('{mysql_url}','INFORMATION_SCHEMA','TABLES','{mysql_un}','{mysql_ps}') where {tb_multi} and TABLE_SCHEMA='{ms_db_name}'"

    tb_list_index = 0
    for tb_multi_name in ch_client.execute(tb_list_sql):
        if tb_list_index == 0:
            c_local_tb_sql = f"""CREATE TABLE IF NOT EXISTS {ch_database}.{ch_tb_name}_local ON CLUSTER {ch_cluster}
                                ENGINE = ReplacingMergeTree() order by {order_id}
                                AS select a.*,b.* from
                                (SELECT * FROM mysql('{mysql_url}','{ms_db_name}','{tb_multi_name[0]}','{mysql_un}','{mysql_ps}')) a ,
                                (select toInt8(1) as data_flag) b where 1=2"""

            ch_client.execute(c_local_tb_sql)

            c_fb_tb_sql = f"""CREATE TABLE IF NOT EXISTS {ch_database}.{ch_tb_name} ON CLUSTER {ch_cluster}
                             as {ch_database}.{ch_tb_name}_local
                             ENGINE = Distributed({ch_cluster},{ch_database},{ch_tb_name}_local,intHash64({order_id}))"""

            ch_client.execute(c_fb_tb_sql)

        logging.info(f'插入表{tb_multi_name[0]}的数据')
        insert_data_sql = f"""insert into {ch_database}.{ch_tb_name}
                            select a.*,b.* from
                            (SELECT * FROM mysql('{mysql_url}','{ms_db_name}','{tb_multi_name[0]}','{mysql_un}','{mysql_ps}')) as a,
                            (select toInt8(1) as data_flag) b"""

        ch_client.execute(insert_data_sql)

        tb_list_index = tb_list_index + 1

def split_table_no_aggregation(tb_multi, ms_db_name, order_id):
    """MySQL分表 clickhouse也分表 数据同步
        :param tb_multi: clickhouse正则函数查询所有分表
        :param ms_db_name: MySQL的数据库
        :param order_id: clickhouse本地表的order_id
    """

    tb_list_sql = f"SELECT TABLE_NAME FROM mysql('{mysql_url}','INFORMATION_SCHEMA','TABLES','{mysql_un}','{mysql_ps}') where {tb_multi} and TABLE_SCHEMA='{ms_db_name}'"


    for tb_multi_name in ch_client.execute(tb_list_sql):

        c_local_tb_sql = f"""CREATE TABLE IF NOT EXISTS {ch_database}.{tb_multi_name[0]}_local ON CLUSTER {ch_cluster}
                            ENGINE = ReplacingMergeTree() order by {order_id}
                            AS select a.*,b.* from
                            (SELECT * FROM mysql('{mysql_url}','{ms_db_name}','{tb_multi_name[0]}','{mysql_un}','{mysql_ps}')) a ,
                            (select toInt8(1) as data_flag) b where 1=2"""

        ch_client.execute(c_local_tb_sql)

        c_fb_tb_sql = f"""CREATE TABLE IF NOT EXISTS {ch_database}.{tb_multi_name[0]} ON CLUSTER {ch_cluster}
                            as {ch_database}.{tb_multi_name[0]}_local
                            ENGINE = Distributed({ch_cluster},{ch_database},{tb_multi_name[0]}_local,intHash64({order_id}))"""

        ch_client.execute(c_fb_tb_sql)

        logging.info(f'插入表{tb_multi_name[0]}的数据')
        insert_data_sql = f"""insert into {ch_database}.{tb_multi_name[0]}
                            select a.*,b.* from
                            (SELECT * FROM mysql('{mysql_url}','{ms_db_name}','{tb_multi_name[0]}','{mysql_un}','{mysql_ps}')) as a,
                            (select toInt8(1) as data_flag) b"""

        ch_client.execute(insert_data_sql)

def sync_table(ms_db_name, ms_dt_name, order_id):
    """MySQL不分表 数据同步
        :param ms_db_name: MySQL的数据库
        :param ms_dt_name: MySQL的表
        :param order_id: clickhouse本地表的order_id
    """

    c_local_tb_sql = f"""CREATE TABLE IF NOT EXISTS {ch_database}.{ms_dt_name}_local ON CLUSTER {ch_cluster}
                        ENGINE = ReplacingMergeTree() order by {order_id}
                        AS select a.*,b.* from
                        (SELECT * FROM mysql('{mysql_url}','{ms_db_name}','{ms_dt_name}','{mysql_un}','{mysql_ps}')) a ,
                        (select toInt8(1) as data_flag) b where 1=2"""

    ch_client.execute(c_local_tb_sql)

    c_fb_tb_sql = f"""CREATE TABLE IF NOT EXISTS {ch_database}.{ms_dt_name} ON CLUSTER {ch_cluster}
                        as {ch_database}.{ms_dt_name}_local
                        ENGINE = Distributed({ch_cluster},{ch_database},{ms_dt_name}_local,intHash64({order_id}))"""

    ch_client.execute(c_fb_tb_sql)

    insert_data_sql = f"""insert into {ch_database}.{ms_dt_name}
                        select a.*,b.* from
                        (SELECT * FROM mysql('{mysql_url}','{ms_db_name}','{ms_dt_name}','{mysql_un}','{mysql_ps}')) as a,
                        (select toInt8(1) as data_flag) b"""

    ch_client.execute(insert_data_sql)

def batch_logic():
    csv = pd.read_csv(os.getenv('RES_PATH') + csv_path, sep = '.', names = ['db','dt','id'])
    for index,row in csv.iterrows():
        logging.info(f"{index}.开始处理{row['dt']}表")

        if re.match(r'^t_oem_carmodel_rel_[0-9]*$', row["dt"]):
            split_table_aggregation(" multiMatchAny(TABLE_NAME, ['^t_oem_carmodel_rel_[0-9]*$']) ",'t_oem_carmodel_rel',row['db'],row['id'])

        if re.match(r'^t_tenant_cm_rel_[0-9]*_[0-9]*$', row["dt"]):
            split_table_aggregation(" multiMatchAny(TABLE_NAME, ['^t_tenant_cm_rel_[0-9]*_[0-9]*$']) ",'t_tenant_cm_rel',row['db'],row['id'])

        if re.match(r'^t_oem_partsku_[0-9]*$', row["dt"]):
            split_table_aggregation(" multiMatchAny(TABLE_NAME, ['^t_oem_partsku_[0-9]*$']) ",'t_oem_partsku',row['db'],row['id'])

        if re.match(r'^t_oem_partsku_code_[0-9]*$', row["dt"]):
            split_table_aggregation(" multiMatchAny(TABLE_NAME, ['^t_oem_partsku_code_[0-9]*$']) ",'t_oem_partsku_code',row['db'],row['id'])

        if re.match(r'^t_standard_oem_rel_[0-9]*$', row["dt"]):
            split_table_aggregation(" multiMatchAny(TABLE_NAME, ['^t_standard_oem_rel_[0-9]*$']) ",'t_standard_oem_rel',row['db'],row['id'])

        if re.match(r'^t_standard_partsku_[0-9]*$', row["dt"]):
            split_table_aggregation(" multiMatchAny(TABLE_NAME, ['^t_standard_partsku_[0-9]*$']) ",'t_standard_partsku',row['db'],row['id'])

        if re.match(r'^t_std_tree_carmodel_rel_[0-9]*$', row["dt"]):
            split_table_aggregation(" multiMatchAny(TABLE_NAME, ['^t_std_tree_carmodel_rel_[0-9]*$']) ",'t_std_tree_carmodel_rel',row['db'],row['id'])

        # t_std_tree_carmodel_每个分表的字段个数不同 在clickhouse保持分表
        if re.match(r'^t_std_tree_carmodel_[0-9]*$', row["dt"]):
            split_table_no_aggregation(" multiMatchAny(TABLE_NAME, ['^t_std_tree_carmodel_[0-9]*$']) ",row['db'],row['id'])

        # 正则匹配纯字母 不分表
        if re.match(r'^[a-zA-Z_]*$', row["dt"]):
            sync_table(row['db'],row["dt"],row['id'])

if __name__ == '__main__':
    kf = "${ods_kafka_offset}"
    pa_kf = 'latest'
    if kf == 'latest':
        pa_kf = 'latest'
    else:
        pa_kf = str(int(datetime.now().timestamp()) * 1000)

    batch_logic()
    ods_ch_start(pa_kf)