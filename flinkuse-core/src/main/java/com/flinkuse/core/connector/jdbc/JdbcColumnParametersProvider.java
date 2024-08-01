package com.flinkuse.core.connector.jdbc;

import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * batchNum 分区数
 * map 分区号：值
 * @author learn
 * @date 2024/7/30 17:37
 */
public class JdbcColumnParametersProvider implements JdbcParameterValuesProvider {

    private final int batchNum;
    private final Map<Integer, List<Long>> value;

    public JdbcColumnParametersProvider(int batchNum, Map<Integer, List<Long>> value) {
        Preconditions.checkArgument(batchNum == value.size(), "value size 要等于 batchNum");
        this.batchNum = batchNum;
        this.value = value;
    }

    @Override
    public Serializable[][] getParameterValues() {
        Serializable[][] parameters = new Serializable[batchNum][1];
        for (Map.Entry<Integer, List<Long>> e : value.entrySet()) {
            parameters[e.getKey()] = e.getValue().toArray(new Long[0]);
        }
        return parameters;
    }

}
