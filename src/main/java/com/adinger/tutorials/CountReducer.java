package com.adinger.tutorials;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class CountReducer implements GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
    @Override
    public void reduce(Iterable<Tuple2<String, Integer>> values,
                       Collector<Tuple2<String, Integer>> collector) {
        int count = 0;
        String fruit = null;

        for (Tuple2<String, Integer> value : values) {
            if (fruit == null) {
                fruit = value.f0;
            }

            count += value.f1;
        }

        collector.collect(new Tuple2<>(fruit, count));

    }
}
