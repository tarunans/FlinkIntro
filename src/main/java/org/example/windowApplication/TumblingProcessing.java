package org.example.windowApplication;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

public class TumblingProcessing {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

    DataStream < String > data = env.socketTextStream("localhost", 9090);
    DataStream < Tuple5 < String, String, String, Integer, Integer >> reduced = data.map(new Splitter())
      .keyBy(t -> t.f0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
      .reduce(new Reduce1());
//    reduced.print();
//    reduced.writeAsText("/home/sitaru/IdeaProjects/Flink_intro/src/main/resources/streamOutput.txt");
    reduced.addSink(
            StreamingFileSink
            .forRowFormat(new Path("/home/sitaru/IdeaProjects/Flink_intro/src/main/resources/streamOutput.txt"),
            new SimpleStringEncoder < Tuple5 < String, String, String, Integer, Integer >> ("UTF-8"))
            .withRollingPolicy(DefaultRollingPolicy.builder().build())
            .build()
    );

    env.execute("Avg Profit Per Month");
  }

  public static class Reduce1 implements ReduceFunction < Tuple5<String, String, String, Integer, Integer>> {
    public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> current,
                      Tuple5<String, String, String, Integer, Integer> pre_result) {
      return new Tuple5(current.f0, current.f1, current.f2, current.f3 + pre_result.f3, current.f4 + pre_result.f4);
    }
  }
  public static class Splitter implements MapFunction < String, Tuple5<String, String, String, Integer, Integer>> {
    public Tuple5<String, String, String, Integer, Integer> map(String value){
        String[] words = value.split(",");
        return new Tuple5 (words[1], words[2], words[3], Integer.parseInt(words[4]), 1);
    }
  }
}
