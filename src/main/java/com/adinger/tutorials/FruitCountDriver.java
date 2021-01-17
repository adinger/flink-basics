package com.adinger.tutorials;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;


public class FruitCountDriver {
    private static final int FRUIT_NAME_POSITION = 0;

    public static void main(String[] args) throws Exception {

        String inputPath = FruitCountDriver.class.getClassLoader().getResource("input.csv").getPath();
        String outputPath = "output";
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(2);
        DataSet<String> data = env.readTextFile(inputPath);

        data
            .flatMap(new TokenizerMapper())
            .partitionByHash(FRUIT_NAME_POSITION)
            .sortPartition(FRUIT_NAME_POSITION, Order.ASCENDING)
            .groupBy(FRUIT_NAME_POSITION)
            .reduceGroup(new CountReducer())
            .writeAsCsv(outputPath);

        JobExecutionResult result = env.execute();
    }

}
