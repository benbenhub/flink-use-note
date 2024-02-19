##http 认证
~~~
    String value = "elastic:byte2022";
    String encodedString = Base64.getEncoder().encodeToString(value.getBytes());
    System.out.println("Authorization = Basic "+encodedString);
~~~

###clickhouse
~~~
alter table v1_scps_basics.ads_standard_partsku_recommend_local on CLUSTER dev_ch_cluster add column `std_tree_id` Int64 AFTER category_id;
alter table v1_scps_basics.ads_standard_partsku_recommend on CLUSTER dev_ch_cluster add column `std_tree_id` Int64 AFTER category_id;

alter table v1_scps_basics.dws_standard_partsku_recommend_local on CLUSTER dev_ch_cluster add column `std_tree_id` Int64 AFTER category_id;
alter table v1_scps_basics.dws_standard_partsku_recommend on CLUSTER dev_ch_cluster add column `std_tree_id` Int64 AFTER category_id;
~~~
###Java加载jar提交
~~~
java -Djava.ext.dirs=${JAVA_LIBS} \
 -cp ${APP_JAR} \
  com.sopei.basics.real.mysqlods.LauncherBuildODSTable -conf ${APP_CONF} 
~~~
###Flink 
~~~
lib/ext-lib
~~~
