package com.adinger.tutorials;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class TokenizerMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) {
        // split each comma-separated line in the file
        String[] tokens = value.toLowerCase().split(",");

        // emit items 0 and 2 in tokens
        collector.collect(new Tuple2<>(tokens[0], Integer.valueOf(tokens[2])));

    }
}
