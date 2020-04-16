package myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkWordCount {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //通过socket获取源数据
        DataStreamSource<String> sourceData = env.socketTextStream("10.22.82.120", 9000);
        /**
         *  数据源进行处理
         *  flatMap方法与spark一样，对数据进行扁平化处理
         *  将每行的单词处理为<word,1>
         */
        DataStream<Tuple2<String, Integer>> dataStream = sourceData.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        })
                // 相同的单词进行分组
                .keyBy(0)
                //  聚合数据
                .sum(1);
        //  将数据流打印到控制台
        dataStream.print();
        /**
         *  Flink与Spark相似，通过action进行出发任务执行，其他的步骤均为lazy模式
         *  这里env.execute就是一个action操作，触发任务执行
         */
        env.execute("streaming word count");
    }
}
