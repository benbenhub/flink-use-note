package com.flinkuse.core.factory;

import com.flinkuse.core.base.ConfigBase;
import com.flinkuse.core.connector.cos.CosOutputFormat;
import com.flinkuse.core.connector.http.HttpUrlOutputFormat;
import com.flinkuse.core.constance.ConfigKeys;
import com.flinkuse.core.enums.JdbcType;
import com.flinkuse.core.connector.elasticsearch7.Elasticsearch7OutputFormat;
import com.flinkuse.core.connector.jdbc.SpJdbcOutputFormat;
import com.flinkuse.core.connector.mongodb.MongodbOutputFormat;
import com.flinkuse.core.connector.mongodb.MongodbUpdateOutputFormat;
import com.flinkuse.core.util.JdbcTypeUtil;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.bson.Document;

import java.lang.reflect.Field;
import java.sql.Types;

/**
 * @author learn
 * @date 2023/1/29 10:34
 */
public class BatchSinkFactory extends ConfigBase {

    public BatchSinkFactory(Configuration scpsConfig) {
        super(scpsConfig);
    }

    public void jdbcSink(DataSet<Row> sink, JdbcType sourceType, String sql) {
        int cc = sql.split("\\?").length - 1;
        jdbcSink(sink, sourceType, sql, getObjectTypes(cc));
    }

    public <T> void jdbcSink(DataSet<Row> sink, JdbcType sourceType, String table, T t) {
        Class<T> c = (Class<T>) t.getClass();

        StringBuilder columnNames = new StringBuilder();
        columnNames.append("INSERT INTO ").append(table).append(" (");
        StringBuilder askCount = new StringBuilder();
        Field[] fields = c.getDeclaredFields();
        int[] fieldTypes = new int[fields.length];
        for (int i=0; i<fields.length; i++) {
            fieldTypes[i] = JdbcTypeUtil.typeInformationToSqlType(BasicTypeInfo.getInfoFor(fields[i].getType()));
            columnNames.append(fields[i].getName()).append(",");
            askCount.append("?,");
        }
        if (fields.length > 0) {
            columnNames.delete(columnNames.length() - 1, columnNames.length());
            askCount.delete(askCount.length() - 1, askCount.length());
        }
        columnNames.append(") values (").append(askCount).append(")");

//        jdbcSink(sink.map(new MapFunction<T, Row>() {
//            @Override
//            public Row map(T value) throws Exception {
//                Field[] fields = value.getClass().getDeclaredFields();
//                Row r = Row.withPositions(fields.length);
//                for (int i=0; i<fields.length; i++) {
//                    r.setField(i,fields[i].get(value));
//                }
//                return r;
//            }
//        }), sourceType, columnNames.toString(), fieldTypes);

        jdbcSink(sink, sourceType, columnNames.toString(), fieldTypes);
    }

    public void jdbcSink(DataSet<Row> sink, JdbcType sourceType, String sql, int[] fieldTypes) {
        switch (sourceType) {
            case mysql:
                sink.output(new SpJdbcOutputFormat(this.scpsConfig).createMySqlConnect(sql, fieldTypes));
                break;
            case clickhouse:
            default:
                sink.output(new SpJdbcOutputFormat(this.scpsConfig).createClickhouseConnect(sql, fieldTypes));
        }
    }

    public void mongodbSink(DataSet<Document> sink, String database, String collection) {
        sink.output(new MongodbOutputFormat(database, collection));
    }

    public void mongodbSink(DataSet<Document> sink, String database, String collection, String[] whereArray) {
        sink.output(new MongodbUpdateOutputFormat(database, collection,whereArray));
    }

    /**
     * sink到elasticsearch
     *
     * @param row DataSet支持任意数据类型，但是要可以转换成Json
     * @param index 索引
     * @param <T>
     */
    public <T> void elasticsearchSink(DataSet<T> row, String index) {
        row.output(new Elasticsearch7OutputFormat<>(index));
    }

    public void httpSink(DataSet<Row> row) {
        row.output(new HttpUrlOutputFormat());
    }

    public void httpSink(DataSet<Row> row,int parallelism) {
        row.output(new HttpUrlOutputFormat()).setParallelism(parallelism);
    }

    public void cosSink(DataSet<Row> row, String cosObject, char delimiter) {
        row.output(new CosOutputFormat(this.scpsConfig.get(ConfigKeys.cos_flink_output_file) + "/" + cosObject
                , delimiter));
    }

    private int[] getObjectTypes(int sum) {
        int[] result = new int[sum];
        for (int i = 0; i < sum; i++) {
            result[i] = Types.JAVA_OBJECT;
        }
        return result;
    }
}
