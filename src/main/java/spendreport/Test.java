package spendreport;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Test
 * StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
 * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 *
 * 设置项目：基本依赖性
 * <dependency>
 *   <groupId>org.apache.flink</groupId>
 *   <artifactId>flink-streaming-java_2.11</artifactId>
 *   <version>1.11.2</version>
 *   <scope>provided</scope>
 * </dependency>
 * 重要提示：请注意，所有这些依赖项的范围都设置为提供。这意味着需要对其进行编译，但不应将它们打包到项目的结果应用程序jar文件中-这些依赖项是Flink Core依赖项，在任何设置中都已经可用。
 *
 *
 * @author Jie.Zhou
 * @date 2020/10/14 16:32
 */
public class Test {
    public static void main(String[] args) throws Exception {
//        testMap();
//        flatMap();

        testTuple();
    }

    /**
     * 显式指定返回类型信息
     */
    private static void testTuple() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.fromElements(1, 2, 3)
                .map(i -> Tuple2.of(i, i*i))    // 没有关于 Tuple2 字段的信息
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .print();
        env.execute("test");
    }

    /**
     * 不幸的是，flatMap()这样的函数，它的签名void flatMap(IN value, Collector<OUT> out)被Java编译器编译为
     * void flatMap(IN value, Collector out)。这样Flink就无法自动写入输出的类型信息了。
     * 在这种情况下，需要显式指定类型信息，否则输出将被视为Object类型，这会导致低效的序列化。
     */
    private static void flatMap() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStreamSource<Integer> input = env.fromElements(1, 2, 3);
        // 必须声明 collector 类型
        input.flatMap((Integer number, Collector<String> out) -> {
            StringBuilder builder = new StringBuilder();
            for(int i = 0; i < number; i++) {
                builder.append("a");
                out.collect(builder.toString());
            }
        }).returns(Types.STRING) // 显式提供类型信息
        .print();// 打印 "a", "a", "aa", "a", "aa", "aaa"
        env.execute("test");
    }

    private static void testMap() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1, 2, 3)
            .map(i -> i*i)
            .print();
        env.execute("test");
    }
}
