package com.flinkuse.cases.connector;

import com.flinkuse.core.base.StreamApp;
import com.flinkuse.core.enums.JdbcConnectionType;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author learn
 * @date 2024/1/26 15:46
 */
public class CosOutClickhouse extends StreamApp {
    public CosOutClickhouse(String[] args) {
        super(args);
    }

    @Override
    public void run(StreamExecutionEnvironment streamEnv) {
        DataStream<String[]> sor = source().cosSource("csv.csv"
        ).flatMap(new FlatMapFunction<String, String[]>() {
            @Override
            public void flatMap(String v, Collector<String[]> out) throws Exception {

                String[] res = v.split("\\|");
                if (res.length == 13)
                    out.collect(res);
            }
        });

        sink().jdbcSink(JdbcConnectionType.clickhouse, sor
                , "INSERT INTO db.tb values(?,?,?,?,?,?,?,?,?,?,?,?,?)"
                , (JdbcStatementBuilder<String[]>) (ps, r) -> {
                    ps.setString(1, r[0]);
                    ps.setString(2, r[1]);
                    ps.setString(3, r[2]);
                    ps.setString(4, r[3]);
                    ps.setString(5, r[4]);
                    ps.setString(6, r[5]);
                    ps.setString(7, r[6]);
                    ps.setString(8, r[7]);
                    ps.setString(9, r[8]);
                    ps.setString(10, r[9]);
                    ps.setString(11, r[10]);
                    ps.setString(12, r[11]);
                    ps.setString(13, r[12]);
                });
    }
}
