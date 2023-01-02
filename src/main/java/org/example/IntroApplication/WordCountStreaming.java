package org.example.IntroApplication;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class WordCountStreaming {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream < String > text = env.socketTextStream("localhost", 9999);
        DataStream < Tuple2 < String, Integer >> counts = text.filter(filterFunction())
                .map(getMapFunction())
                .keyBy(t -> t.f0)
                .sum(1);
        counts.print();
        env.execute("Streaming WordCount");
    }

    private static MapFunction < String, Tuple2 < String, Integer >> getMapFunction(){
        return new MapFunction < String, Tuple2 < String, Integer >>(){
            @Override
            public Tuple2 < String, Integer > map(String value) {
                return new Tuple2 < String, Integer > (value, Integer.valueOf(1));
            }};
    }

    private static FilterFunction < String > filterFunction(){
        return new FilterFunction < String > () {
            @Override
            public boolean filter(String value) {
                return value.startsWith("N");
            }
        };
    }
}
