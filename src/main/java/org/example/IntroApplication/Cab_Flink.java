package org.example.IntroApplication;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.guava30.com.google.common.base.Strings;

import java.util.Arrays;
import java.util.List;

public class Cab_Flink {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);
        List<Tuple2<String, Double>> text = env.readTextFile(params.get("input"))
                .filter(filterFunction())
                .map(splitter())
                .groupBy(0)
                .reduce(reduceFunction())
                .map(averageRideMapper()).aggregate(Aggregations.MAX, 1).collect();
        for (Tuple2<String, Double> sum : text) {
            System.out.println(sum);
        }
    }


    private static KeySelector<Tuple2<String, Integer>, String> keyMapper(){
        return new KeySelector<Tuple2<String, Integer>, String>(){
            @Override
            public String getKey(Tuple2<String, Integer> input){
                return input.f0;
            }
        };
    }

    private static MapFunction <Tuple3<String, Integer, Integer>, Tuple2<String, Double>> averageRideMapper(){
        return new MapFunction <Tuple3<String, Integer, Integer>, Tuple2<String, Double>>(){
            @Override
            public Tuple2<String, Double> map(Tuple3<String, Integer, Integer> value) {
                return Tuple2.of(value.f0, value.f1.doubleValue()/value.f2.doubleValue());
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

    private static ReduceFunction<Tuple3<String, Integer, Integer>> reduceFunction(){
        return new ReduceFunction<Tuple3<String, Integer, Integer>>(){
            @Override
            public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> current, Tuple3<String, Integer, Integer> pre_result) {
                return Tuple3.of(current.f0, current.f1 + pre_result.f1,  current.f2 + pre_result.f2);
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
