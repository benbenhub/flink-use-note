package com.flinkuse.cases.connector;

import com.flinkuse.core.base.StreamApp;
import com.flinkuse.core.enums.OdsBasicsConf;
import com.flinkuse.core.modul.BinlogBean;
import com.flinkuse.core.serialization.KafkaSerializationBinlog;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author learn
 * @date 2022/3/24 18:41
 */
public class BinlogOutKafka extends StreamApp {

    public BinlogOutKafka(String[] args) {
        super(args);
    }

    public static void main(String[] args) throws Exception {
        new BinlogOutKafka(args).start();
    }
    @Override
    public void run(StreamExecutionEnvironment streamEnv) {
        setupCheckpoint();//开启Checkpoint
        //创建数据源
        DataStream<BinlogBean> source = source().mysqlCdcSource(StartupOptions.latest());

//        DataStream<BinlogBean> dataFieldSetup = source.flatMap(new FlatMapFunction<BinlogBean, BinlogBean>() {
//            @Override
//            public void flatMap(BinlogBean value, Collector<BinlogBean> out) throws Exception {
//                String tabName = value.getTableName();
//                // 把带有数字的表名的数字删掉
//                tabName = tabName.replaceAll("\\d+","");
//                // 递归删除最后的下划线
//                value.setTableName(lastUnderlineDelete(tabName));
//
//                out.collect(value);
//            }
//        });

        sink().kafkaSink(source,
                KafkaRecordSerializationSchema.builder()
                .setTopic(OdsBasicsConf.binlogTopic)
                .setValueSerializationSchema(new KafkaSerializationBinlog())
                .build());
    }
}
