package com.flinkuse.core.connector.jdbc;

import com.flinkuse.core.base.ConfigBase;
import com.flinkuse.core.constance.ConfigKeys;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;

/**
 * @author learn
 * @date 2023/2/1 10:23
 */
public class SpJdbcInputFormat extends ConfigBase {
    private JdbcParameterValuesProvider jdbcParameterValuesProvider;
    public SpJdbcInputFormat(Configuration scpsConfig) {
        super(scpsConfig);
    }
    public SpJdbcInputFormat(Configuration scpsConfig, JdbcParameterValuesProvider jdbcParameterValuesProvider) {
        super(scpsConfig);
        this.jdbcParameterValuesProvider = jdbcParameterValuesProvider;
    }
    public JdbcInputFormat createMySqlConnect(String sql, RowTypeInfo rowTypeInfo) {
        JdbcInputFormat.JdbcInputFormatBuilder jifb = new JdbcInputFormat.JdbcInputFormatBuilder()
                .setDBUrl(String.format("jdbc:mysql://%s:%s/%s?%s",
                        this.scpsConfig.get(ConfigKeys.mysql_host)
                        , this.scpsConfig.get(ConfigKeys.mysql_port)
                        , this.scpsConfig.get(ConfigKeys.mysql_database)
                        , "socket_timeout=" + this.scpsConfig.get(ConfigKeys.mysql_socket_timeout)))
                .setDrivername(this.scpsConfig.get(ConfigKeys.mysql_driver))
                .setUsername(this.scpsConfig.get(ConfigKeys.mysql_username))
                .setPassword(this.scpsConfig.get(ConfigKeys.mysql_password))
                .setQuery(sql)
                .setRowTypeInfo(rowTypeInfo);
        if (jdbcParameterValuesProvider != null)
            jifb.setParametersProvider(jdbcParameterValuesProvider);
        return jifb.finish();
    }

    public JdbcInputFormat createClickhouseConnect(String sql, RowTypeInfo rowTypeInfo) {
        JdbcInputFormat.JdbcInputFormatBuilder jifb = new JdbcInputFormat.JdbcInputFormatBuilder()
                .setDBUrl(String.format("jdbc:clickhouse://%s:%s/%s?%s",
                        this.scpsConfig.get(ConfigKeys.clickhouse_host)
                        , this.scpsConfig.get(ConfigKeys.clickhouse_port)
                        , this.scpsConfig.get(ConfigKeys.clickhouse_database)
                        , "socket_timeout=" + this.scpsConfig.get(ConfigKeys.clickhouse_socket_timeout)))
                .setDrivername(this.scpsConfig.get(ConfigKeys.clickhouse_driver))
                .setUsername(this.scpsConfig.get(ConfigKeys.clickhouse_username))
                .setPassword(this.scpsConfig.get(ConfigKeys.clickhouse_password))
                .setQuery(sql)
                .setRowTypeInfo(rowTypeInfo);
        if (jdbcParameterValuesProvider != null)
            jifb.setParametersProvider(jdbcParameterValuesProvider);
        return jifb.finish();
    }

    public JdbcInputFormat createDorisConnect(String sql, RowTypeInfo rowTypeInfo) {
        JdbcInputFormat.JdbcInputFormatBuilder jifb = new JdbcInputFormat.JdbcInputFormatBuilder()
                .setDBUrl(String.format("jdbc:mysql://%s:%s/%s?%s",
                        this.scpsConfig.get(ConfigKeys.doris_host)
                        , this.scpsConfig.get(ConfigKeys.doris_port)
                        , this.scpsConfig.get(ConfigKeys.doris_database)
                        , "socket_timeout="
                                + this.scpsConfig.get(ConfigKeys.doris_socket_timeout)))
                .setDrivername(this.scpsConfig.get(ConfigKeys.doris_driver))
                .setUsername(this.scpsConfig.get(ConfigKeys.doris_username))
                .setPassword(this.scpsConfig.get(ConfigKeys.doris_password))
                .setQuery(sql)
                .setRowTypeInfo(rowTypeInfo);
        if (jdbcParameterValuesProvider != null)
            jifb.setParametersProvider(jdbcParameterValuesProvider);
        return jifb.finish();
    }
}
