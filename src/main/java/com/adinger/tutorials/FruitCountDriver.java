package com.adinger.tutorials;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;


@Slf4j
public class FruitCountDriver {
    private static final int FRUIT_NAME_POSITION = 0;

    public static void main(String[] args) throws Exception {

        String inputPath = FruitCountDriver.class.getClassLoader().getResource("input.csv").getPath();
        String outputPath = "output";
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        DataSet<String> data = env.readTextFile(inputPath);

        data
            .flatMap(new Tokenizer())
            .setParallelism(2)
            .partitionByHash(FRUIT_NAME_POSITION)
            .sortPartition(FRUIT_NAME_POSITION, Order.ASCENDING)
            .groupBy(0)
            .reduceGroup(new CountReducer())
            .writeAsCsv(outputPath);

        JobExecutionResult result = env.execute();
        log.info("JobExecutionResult: {}", result.toString());

    }

}
