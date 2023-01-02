package org.example.IntroApplication;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class AverageProfit {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStreamText = env.readTextFile("/home/sitaru/IdeaProjects/Flink_intro/src/main/resources/avg.txt");
        DataStream<List<String>> profitPerMonth = dataStreamText
                .map(splitter())
                .keyBy(keyMapper())
                .reduce(reduceFunction())
                .map(profitMapper());

        profitPerMonth.writeAsText("/home/sitaru/IdeaProjects/Flink_intro/src/main/resources/outputAverageProfit.txt");
        profitPerMonth.print();
        env.execute("Streaming WordCount");
    }

    private static MapFunction <String, List<String>> splitter(){
        return new MapFunction <String, List<String>>(){
            @Override
            public List<String> map(String value) {
                String[] words = value.split(",");
                return Arrays.asList(words[1], words[2], words[3], words[4], "1");
            }
        };
    }

    private static MapFunction<List<String>, List<String>> profitMapper(){
        return new MapFunction<List<String>, List<String>>(){
            @Override
            public List<String> map(List<String> input) {
                return Arrays.asList(input.get(0), String.valueOf(Double.parseDouble(input.get(3))));
            }
        };
    }

    private static KeySelector<List<String>, String> keyMapper(){
        return new KeySelector<List<String>, String>(){
            @Override
            public String getKey(List<String> input){
                return input.get(0);
            }
        };
    }

    private static ReduceFunction<List<String>> reduceFunction(){
        return new ReduceFunction<List<String>>(){
            @Override
            public List<String> reduce(List<String> current, List<String> pre_result) {
                return Arrays.asList(current.get(0), current.get(1), current.get(2),
                        String.valueOf(Integer.parseInt(current.get(3)) + Integer.parseInt(pre_result.get(3))),
                        String.valueOf(Integer.parseInt(current.get(4)) + Integer.parseInt(pre_result.get(4))));
            }
        };
    }

}
