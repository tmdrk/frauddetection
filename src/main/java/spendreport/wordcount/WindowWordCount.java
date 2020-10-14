package spendreport.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * WindowWordCount
 *
 * @author Jie.Zhou
 * @date 2020/10/13 15:47
 */
public class WindowWordCount {
    public static void main(String[] args) throws Exception {
        //参数
//        String propertiesFilePath = "/home/sam/flink/myjob.properties";
//        ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFilePath);
//        ParameterTool parameter = ParameterTool.fromArgs(args);
//        ParameterTool parameter = ParameterTool.fromSystemProperties();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
//                .setParallelism(2) //设置并行线程数
                .keyBy(value -> value.f0)
//                .timeWindow(Time.seconds(20)) //定义一个20秒的翻滚窗口
                .timeWindow(Time.seconds(20), Time.seconds(10))  // 定义了一个滑动窗口，窗口大小为20秒，每10秒滑动一次
                .sum(1);
        dataStream.print();
        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
