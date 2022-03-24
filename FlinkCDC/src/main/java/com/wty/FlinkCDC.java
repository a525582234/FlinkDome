package com.wty;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {

    public static void main(String[] args) throws Exception {

        //1.获取Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 开启CK
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));

        env.setStateBackend(new FsStateBackend("hdfs://192.168.111.103:8020/FlinkCDC"));

        //2.通过FlinkCDC构建SourceFunction
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("192.168.111.103")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("cdc_test")
                .tableList("cdc_test.user_info")
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        //3.数据打印
        dataStreamSource.print();

        //4.启动任务
        env.execute("FlinkCDC");

    }

}
