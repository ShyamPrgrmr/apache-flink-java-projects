package com.flink.learning.projects.project_4;

import org.apache.flink.api.common.functions.AggregateFunction;

import com.flink.learning.projects.data_injector.models.CPU;


public class CpuAverageAggregator
        implements AggregateFunction<CPU, CpuAccumulator, Double> {

    @Override
    public CpuAccumulator createAccumulator() {
        return new CpuAccumulator();
    }

    @Override
    public CpuAccumulator add(CPU value, CpuAccumulator acc) {
        acc.sum += value.getUsage();
        acc.count++;
        return acc;
    }

    @Override
    public Double getResult(CpuAccumulator acc) {
        return acc.sum / acc.count;
    }

    @Override
    public CpuAccumulator merge(CpuAccumulator a, CpuAccumulator b) {
        CpuAccumulator merged = new CpuAccumulator();
        merged.sum = a.sum + b.sum;
        merged.count = a.count + b.count;
        return merged;
    }
}
