package org.example.StockApplication;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

public class StockMain {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<List < String >> data = env
//                .socketTextStream("localhost", 9090)
                .readTextFile("/home/sitaru/IdeaProjects/Flink_intro/src/main/resources/FUTURES_TRADES.txt")
                .map(getInputMapper())
                .assignTimestampsAndWatermarks(getWatermarkStrategy());

        DataStream <List< String >> alarmedCustTransactions = data
                .keyBy(t -> t.get(0))
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .reduce(reduceFunction());

//        DataStream <List< String >> AllFlaggedTxn =
//                alarmedCustTransactions.union(data, alarmedCustTransactions);
        alarmedCustTransactions.addSink(StreamingFileSink
                .forRowFormat(new Path("/home/sitaru/IdeaProjects/Flink_intro/src/main/resources/flagged_stock"),
                        new SimpleStringEncoder< List < String >>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder().build())
                .build());
        // execute program
        env.execute("Streaming Stock");

    }

    private static ReduceFunction<List<String>> reduceFunction(){
        return new ReduceFunction<List<String>>(){
            @Override
            public List<String> reduce(List<String> current, List<String> pre_result) {
                if( Double.parseDouble(current.get(2)) > Double.parseDouble(pre_result.get(2)) ){
                    return current;
                }
                return pre_result;
            }
        };
    }

    private static MapFunction<String, List<String>> getInputMapper(){
        return new MapFunction<String, List<String>>() {
            public List < String > map(String value) {
                String[] words = value.split(",");
                LocalDateTime eventTime = LocalDateTime.from(DateTimeFormatter.ofPattern("dd/MM/yyyy,HH:mm:ss")
                        .parse(words[0] + "," + words[1]));
                long eventMilli = eventTime.atZone(ZoneId.systemDefault())
                        .toInstant().toEpochMilli();
                return Arrays.asList(words[0], String.valueOf(eventMilli), words[2], words[3]);
            }
        };
    }

    private static WatermarkStrategy<List<String>> getWatermarkStrategy(){
        return WatermarkStrategy.< List<String>> forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> {
                    try {
                        return Long.parseLong(event.get(1));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return 0;
                });
    }

}
