package com.wty.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理word count
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行文件
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //从文件中读取数据
        String inputPath = "E:\\dev\\FlinkDome\\src\\main\\resources\\hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        //对数据集进行处理，按照空格分词展开，转换成(word,1)这种二元组进行统计
        DataSet<Tuple2<String,Integer>> resultSet = inputDataSet.flatMap( new MyflatMapper() )
                //按照第一个位置word分组
                .groupBy(0)
                //将第二个位置上的数据进行求和
                .sum(1);
        resultSet.print();
    }
    //自定义类，实现FlatMapFunction接口
    public static class MyflatMapper implements FlatMapFunction<String, Tuple2<String,Integer>>{
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //按空格分词
            String[] worlds = value.split(" ");
            //遍历所有word 包成二元组输出
            for (String world : worlds) {
                out.collect(new Tuple2<>(world,1));
            }
        }
    }
}
