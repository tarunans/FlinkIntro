package org.example.windowApplication;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.guava30.com.google.common.base.Strings;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class CabFlinkTublingProcessing {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<Tuple3<String, Integer, Integer>> sumMonth = env
                .readTextFile("/home/sitaru/IdeaProjects/Flink_intro/src/main/resources/cab_flink.txt")
//               .socketTextStream("localhost", 9090)
                .filter(filterFunction())
                .map(splitter())
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .reduce(new SumReduce());

        DataStream<Tuple3<String, Integer, Integer>> maxEntries = sumMonth
                .keyBy(t -> t.f2)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .reduce(new MaxReduce());

        sumMonth.addSink(getFileSinker("/home/sitaru/IdeaProjects/Flink_intro/src/main/resources/sumOutput.txt"))
                .setParallelism(1)
                .name("SumOutputSink");

        maxEntries.print();

        maxEntries.addSink(getFileSinker("/home/sitaru/IdeaProjects/Flink_intro/src/main/resources/maxSumOutput.txt"))
                .setParallelism(1)
                .name("MaxSumOutputSink");

        env.execute("Avg Profit Per Month");
        return;
    }

    private static StreamingFileSink getFileSinker(String targetLoc){
        return StreamingFileSink.forRowFormat(new Path(targetLoc),
                        new SimpleStringEncoder<Tuple3<String, Integer, Integer>>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder().build())
                .build();
    }

    public static class MaxReduce implements ReduceFunction<Tuple3<String, Integer, Integer>> {
        public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> current,
                                                       Tuple3<String, Integer, Integer> pre_result) {
            return pre_result.f1 > current.f1 ? pre_result : current;
        }
    }

    public static class SumReduce implements ReduceFunction<Tuple3<String, Integer, Integer>> {
        public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> current,
                                                                       Tuple3<String, Integer, Integer> pre_result) {
            return new Tuple3<>(current.f0, current.f1 + pre_result.f1, 1);
        }
    }

    private static ProcessFunction<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>> getProcess(){
        return new ProcessFunction<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>() {
            private Map<String, Tuple2<Integer,Integer>> sumValues = new HashMap<>();

            @Override
            public void processElement(Tuple3<String, Integer, Integer> value, Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                String key = value.f0;
                Integer val1 = value.f1;
                Integer val2 = value.f2;

                Tuple2<Integer,Integer> result = sumValues.getOrDefault(key, Tuple2.of(0, 0));
                Tuple2<Integer,Integer> newResult = Tuple2.of(val1 + result.f0, val2 + result.f1);
                sumValues.put(key, newResult);

                // output the maximum value for this key
                out.collect(Tuple3.of(key, newResult.f0, newResult.f1));
            }
        };
    }

    private static MapFunction<String, Tuple3<String, Integer, Integer>> splitter(){
        return new MapFunction <String, Tuple3<String, Integer, Integer>>(){
            @Override
            public Tuple3<String, Integer, Integer> map(String value) {
                String[] words = value.split(",");
                return Tuple3.of(words[6], Integer.parseInt(words[7]), 1);
            }
        };
    }

    private static FilterFunction<String> filterFunction(){
        return new FilterFunction <String> () {
            @Override
            public boolean filter(String value) {
                return !(Strings.isNullOrEmpty(value) || value.contains("null"));
            }
        };
    }

}
