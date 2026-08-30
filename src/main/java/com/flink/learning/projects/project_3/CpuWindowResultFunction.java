package com.flink.learning.projects.project_3;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CpuWindowResultFunction
    extends ProcessWindowFunction<Double, String, String, TimeWindow> {

    @Override
    public void process(
            String cpuName,
            Context context,
            Iterable<Double> average,
            Collector<String> out) {

        out.collect(cpuName + ": " +
                String.format("%.2f", average.iterator().next()) + "%");
    }
}