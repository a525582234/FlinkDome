package com.wty.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception{
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置线程数
        env.setParallelism(8);

//        //从文件中读取数据
        String inputPath = "E:\\dev\\FlinkDome\\src\\main\\resources\\hello.txt";
        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 用parameter tool 工具从程序启动参数中提取配置项
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String host = parameterTool.get("host");
//        int port = parameterTool.getInt("port");

        //正常生产环境是使用消息队列
        //从socket文本流读取数据 linux 自带小工具，nc -lk 7777
//        DataStream<String> inputDataStream = env.socketTextStream("192.168.111.128",7777);

        //基于数据流就行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyflatMapper())
                .keyBy(0).sum(1);

        resultStream.print();

        //执行任务
        env.execute();
    }
}
