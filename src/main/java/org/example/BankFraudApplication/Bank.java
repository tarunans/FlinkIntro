package org.example.BankFraudApplication;

import java.util.Map;

import org.apache.flink.util.Collector;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction.ReadOnlyContext;

import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

public class Bank {
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // transaction data keyed by customer_id
    DataStream < Tuple2 < String, String >> data = env.socketTextStream("localhost", 9090)
      .map(new MapFunction < String, Tuple2 < String, String >> () {
        public Tuple2 < String, String > map(String value) {
          String[] words = value.split(",");
          return new Tuple2 < String, String > (words[3], value); //{(id_347hfx) (HFXR347924,2018-06-14 23:32:23,Chandigarh,id_347hfx,hf98678167,123302773033,774
        }
      });

    // (1) Check against alarmed customers
    DataStream < Tuple2 < String, String >> alarmedCustTransactions = data
      .keyBy(t -> t.f0)
      .process(new AlarmProcess());

    // (2) Check against lost cards
    DataStream < Tuple2 < String, String >> lostCardTransactions = data
      .keyBy(t -> t.f0)
      .process(new LostCardProcess());

    DataStream < Tuple2 < String, String >> excessiveTransactions = data
      // (3) More than 10 transactions check
      .map(new MapFunction < Tuple2 < String, String > , Tuple3 < String, String, Integer >> () {
        public Tuple3 < String, String, Integer > map(Tuple2 < String, String > value) {
          return new Tuple3 < String, String, Integer > (value.f0, value.f1, 1);
        }
      })
      .keyBy(t -> t.f0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .sum(2)
      .flatMap(new FilterAndMapMoreThan10());

    DataStream < Tuple2 < String, String >> freqCityChangeTransactions = data
      //.keyBy(t -> t.f0)
      .keyBy(new KeySelector < Tuple2 < String, String > , String > () {
        public String getKey(Tuple2 < String, String > value) {
          return value.f0;
        }
      })
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .process(new Citychange());

    DataStream < Tuple2 < String, String >> AllFlaggedTxn =
      alarmedCustTransactions.union(lostCardTransactions, excessiveTransactions, freqCityChangeTransactions);

    AllFlaggedTxn.addSink(StreamingFileSink
      .forRowFormat(new Path("/home/sitaru/IdeaProjects/Flink_intro/src/main/resources/flagged_transaction"),
        new SimpleStringEncoder < Tuple2 < String, String >> ("UTF-8"))
      .withRollingPolicy(DefaultRollingPolicy.builder().build())
      .build());
    // execute program
    env.execute("Streaming Bank");
  }

  public static class Citychange extends ProcessWindowFunction < Tuple2 < String, String > , Tuple2 < String, String > , String, TimeWindow > {
    public void process(String key, Context context, Iterable < Tuple2 < String, String >> input, Collector < Tuple2 < String, String >> out) {
      String lastCity = "";
      int changeCount = 0;
      for (Tuple2 < String, String > element: input) {
        String city = element.f1.split(",")[2].toLowerCase();

        if (lastCity.isEmpty()) {
          lastCity = city;
        } else {
          if (!city.equals(lastCity)) {
            lastCity = city;
            changeCount += 1;
          }
        }

        if (changeCount >= 2) {
          out.collect(new Tuple2 < String, String > ("__ALARM__", element + "marked for FREQUENT city changes"));
        }
      }
    }
  }

  public static class FilterAndMapMoreThan10 implements FlatMapFunction < Tuple3 < String, String, Integer > , Tuple2 < String, String >> {
    public void flatMap(Tuple3 < String, String, Integer > value, Collector < Tuple2 < String, String >> out) {
      if (value.f2 > 10) {
        out.collect(new Tuple2 < String, String > ("__ALARM__", value + " marked for >10 TXNs"));
      }
    }
  }

}