package com.flink.learning.projects.project_4;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CpuWindowResultFunction
    extends ProcessWindowFunction<Double, String, String, TimeWindow> {

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    @Override
    public void process(
            String cpuName,
            Context context,
            Iterable<Double> average,
            Collector<String> out) {
        

        String windowStart = Instant.ofEpochMilli(context.window().getStart())
            .atZone(ZoneId.systemDefault())
            .toLocalTime()
            .format(TIME_FORMATTER);

        String windowEnd = Instant.ofEpochMilli(context.window().getEnd())
            .atZone(ZoneId.systemDefault())
            .toLocalTime()
            .format(TIME_FORMATTER);

        long wm = context.currentWatermark();

        String watermark = Instant.ofEpochMilli(wm)
                .atZone(ZoneId.systemDefault())
                .toLocalTime()
                .format(TIME_FORMATTER);

        out.collect(
            cpuName +
            " | Watermark: " + watermark +
            " | Window: " + windowStart +
            " - " + windowEnd +
            " | Avg: " + String.format("%.2f", average.iterator().next()) + "%"
        );
        
    }
}