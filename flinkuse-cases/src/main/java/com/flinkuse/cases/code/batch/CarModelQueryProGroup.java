package com.flinkuse.cases.code.batch;

import com.flinkuse.core.base.BatchApp;
import com.flinkuse.core.enums.JdbcConnectionType;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * @author learn
 * @date 2022/8/17 14:15
 */
public class CarModelQueryProGroup extends BatchApp {

    public CarModelQueryProGroup(String[] args) {
        super(args);
    }

    public static void main(String[] args) throws Exception {
        CarModelQueryProGroup carModelQueryProGroup = new CarModelQueryProGroup(args);
        carModelQueryProGroup.start();
    }
    @Override
    public void run(ExecutionEnvironment batchEnv) {
        String addIndex = getScpsParams().get("caraddIndex");
        String cmQuerySql =
                "SELECT \n" +
                        "  cm.cm_brand,\n" +
                        "  cm.cm_brand_country,\n" +
                        "  cm.cm_brand_letter,\n" +
                        "  cm.cm_factory,\n" +
                        "  cm.cm_factory_letter,\n" +
                        "  cm.cm_car,\n" +
                        "  cm.cm_model,\n" +
                        "  cm.cm_model_letter,\n" +
                        "  cm.cm_brand_id, \n" +
                        "  brand.cm_brand_image_url,\n" +
                        "  brand.cm_hot_flag,\n" +
                        "  brand.cm_brand_status,\n" +
                        "  factory.cm_factory_index \n" +
                        "FROM\n" +
                        "  (SELECT \n" +
                        "    cm_brand,\n" +
                        "    cm_brand_country,\n" +
                        "    cm_brand_letter,\n" +
                        "    cm_factory_letter,\n" +
                        "    cm_model_letter,\n" +
                        "    cm_factory,\n" +
                        "    cm_factory_id,\n" +
                        "    cm_car,\n" +
                        "    cm_model,\n" +
                        "    cm_brand_id \n" +
                        "  FROM\n" +
                        "    scps_mysql_ods.t_carmodel_base final\n" +
                        "  WHERE data_flag > 0 \n" +
                        "  GROUP BY cm_brand,\n" +
                        "    cm_brand_country,\n" +
                        "    cm_brand_letter,\n" +
                        "    cm_factory_letter,\n" +
                        "    cm_model_letter,\n" +
                        "    cm_factory,\n" +
                        "    cm_factory_id,\n" +
                        "    cm_car,\n" +
                        "    cm_model,\n" +
                        "    cm_brand_id ) AS cm \n" +
                        "  JOIN \n" +
                        "    (SELECT \n" +
                        "      cm_brand_id,\n" +
                        "      cm_brand_image_url,\n" +
                        "      cm_hot_flag,\n" +
                        "      cm_brand_status \n" +
                        "    FROM\n" +
                        "      scps_mysql_ods.t_carmodel_brand final where data_flag > 0) brand \n" +
                        "    ON brand.cm_brand_id = cm.cm_brand_id \n" +
                        "  JOIN \n" +
                        "    (SELECT \n" +
                        "      cm_factory_id,\n" +
                        "      cm_factory_index \n" +
                        "    FROM\n" +
                        "      scps_mysql_ods.t_carmodel_factory final where data_flag > 0) factory \n" +
                        "    ON factory.cm_factory_id = cm.cm_factory_id ";

        DataSet<Map<String, Object>> cmDataSet = source().jdbcSource( JdbcConnectionType.clickhouse,cmQuerySql
                ,new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO
                        ,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO
                        ,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO
                        ,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO
                ))
                .map(new MapFunction<Row, Map<String, Object>>() {
                    @Override
                    public Map<String, Object> map(Row row)  {
                        HashMap<String, Object> map = new HashMap<>();
                        map.put("cm_brand",row.getField(0));
                        map.put("cm_brand_country",row.getField(1));
                        map.put("cm_brand_letter",row.getField(2));
                        map.put("cm_factory",row.getField(3));
                        map.put("cm_factory_letter",row.getField(4));
                        map.put("cm_car",row.getField(5));
                        map.put("cm_model",row.getField(6));
                        map.put("cm_model_letter",row.getField(7));
                        map.put("cm_brand_id",String.valueOf(row.getField(8)));
                        map.put("cm_brand_image_url",row.getField(9));
                        map.put("cm_hot_flag",row.getField(10));
                        map.put("search_key","" + row.getField(0) + " " + row.getField(3) + " " +  (row.getField(6) + "").replaceAll("-",""));
                        map.put("cm_brand_status","" + row.getField(11));
                        map.put("cm_factory_index","" + row.getField(12));
                        return map;
                    }
                });

        sink().elasticsearchSink(cmDataSet,"basics_carmodel_display"+ addIndex);
    }
}
