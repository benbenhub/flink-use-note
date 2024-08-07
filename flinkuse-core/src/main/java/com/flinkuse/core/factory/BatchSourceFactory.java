package com.flinkuse.core.factory;

import com.flinkuse.core.base.ConfigBase;
import com.flinkuse.core.connector.elasticsearch7.Elasticsearch7InputFormat;
import com.flinkuse.core.connector.jdbc.JdbcColumnParametersProvider;
import com.flinkuse.core.connector.jdbc.SpJdbcInputFormat;
import com.flinkuse.core.connector.mongodb.MongodbInputFormat;
import com.flinkuse.core.enums.JdbcType;
import com.flinkuse.core.modul.SqlColumn;
import com.flinkuse.core.util.SQLParseUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.http.util.TextUtils;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author learn
 * @date 2023/1/29 11:05
 */
public class BatchSourceFactory extends ConfigBase {

    private static ExecutionEnvironment env;

    public BatchSourceFactory(Configuration scpsConfig, ExecutionEnvironment env) {
        super(scpsConfig);
        BatchSourceFactory.env = env;
    }

    public DataSet<Row> jdbcSource(JdbcType sourceType, String sql) throws Exception {
        return rowArrayTurnMap(jdbcSource(sourceType, sql, getObjectRowTypeInfo(SQLParseUtil.parseSelect(sql,sourceType))));
    }

    /**
     * 分区设置
     * @param sourceType
     * @param sql
     * @param parallelismValue 分区号：sql对应的值
     * @return
     * @throws Exception
     */
    public DataSet<Row> jdbcSourceColVal(JdbcType sourceType,
                                         String sql,
                                         Map<Integer, List<Long>> parallelismValue) throws Exception {
        switch (sourceType) {
            case mysql:
                return rowArrayTurnMap(env.createInput(
                        new SpJdbcInputFormat(this.scpsConfig,
                                new JdbcColumnParametersProvider(env.getParallelism(), parallelismValue)
                        ).createMySqlConnect(sql, getObjectRowTypeInfo(SQLParseUtil.parseSelect(sql,sourceType)))));
            case doris:
                return rowArrayTurnMap(env.createInput(
                        new SpJdbcInputFormat(this.scpsConfig,
                                new JdbcColumnParametersProvider(env.getParallelism(), parallelismValue)
                        ).createDorisConnect(sql, getObjectRowTypeInfo(SQLParseUtil.parseSelect(sql,sourceType)))));
            case clickhouse:
            default:
                return rowArrayTurnMap(env.createInput(
                        new SpJdbcInputFormat(this.scpsConfig,
                                new JdbcColumnParametersProvider(env.getParallelism(), parallelismValue)
                        ).createClickhouseConnect(sql, getObjectRowTypeInfo(SQLParseUtil.parseSelect(sql,sourceType)))));
        }
    }

    public <T> DataSet<T> jdbcSource(JdbcType sourceType, String sql, T t) throws Exception {
        Class<T> c = (Class<T>) t.getClass();

        Field[] fields = c.getDeclaredFields();
        TypeInformation<?>[] rowTypes = new TypeInformation[fields.length];
        for (int i=0; i<fields.length; i++) {
            rowTypes[i] = BasicTypeInfo.getInfoFor(fields[i].getType());
        }

        return (DataSet<T>) jdbcSource(sourceType, sql, new RowTypeInfo(rowTypes)
        ).map(new MapFunction<Row, Object>() {
            @Override
            public Object map(Row value) throws Exception {
                T t1 = c.getDeclaredConstructor().newInstance();
                Field[] fields1 = c.getDeclaredFields();
                for (int i=0; i<value.getArity(); i++) {
                    Field field = fields1[i];
                    field.setAccessible(true);
                    field.set(t1, value.getField(i));
                }
                return t1;
            }
        });
    }
    /**
     * 创建InputFormat获取源数据
     *
     * @param sourceType
     * @param sql
     * @param rowTypeInfo
     * @return
     */
    public DataSet<Row> jdbcSource(
            JdbcType sourceType
            , String sql
            , RowTypeInfo rowTypeInfo
    ) {
        switch (sourceType) {
            case mysql:
                return env.createInput(new SpJdbcInputFormat(this.scpsConfig).createMySqlConnect(sql, rowTypeInfo));
            case doris:
                return env.createInput(new SpJdbcInputFormat(this.scpsConfig).createDorisConnect(sql, rowTypeInfo));
            case clickhouse:
            default:
                return env.createInput(new SpJdbcInputFormat(this.scpsConfig).createClickhouseConnect(sql, rowTypeInfo));
        }
    }

    public DataSet<Map<String, Object>> elasticsearchSource(String index, String query, String... includes) {
        return env.createInput(new Elasticsearch7InputFormat(index, query, includes));
    }

    public DataSet<Map<String, Object>> mongodbSource(String database,String collection, String filter) {
        return env.createInput(new MongodbInputFormat(database, collection, filter, scpsConfig));
    }

    // 把数组Row转换为MapRow
    private DataSet<Row> rowArrayTurnMap(DataSet<Row> in) {
        return in.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {
                Row row = Row.withNames();
                for (String field : Objects.requireNonNull(value.getFieldNames(true))) {
                    row.setField(field, value.getField(field));
                }
                return row;
            }
        });
    }

    private RowTypeInfo getObjectRowTypeInfo(List<SqlColumn> columns) {
        TypeInformation<Object>[] typeInformations = new TypeInformation[columns.size()];
        String[] fieldNames = new String[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            typeInformations[i] = TypeInformation.of(Object.class);
            SqlColumn sqlColumn = columns.get(i);
            fieldNames[i] = TextUtils.isEmpty(sqlColumn.getColumnAlias())? sqlColumn.getColumnName() : sqlColumn.getColumnAlias();
        }

        return new RowTypeInfo(typeInformations,fieldNames);
    }

}
