package com.flink.learning.projects.project_2;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.flink.learning.projects.data_injector.models.User;

public class UserCountProcessFunction
        extends KeyedProcessFunction<String, User, User> {

    private ValueState<Integer> countState;

    @Override
    public void open(Configuration parameters) {

        ValueStateDescriptor<Integer> descriptor =
                new ValueStateDescriptor<>("event-count", Integer.class);

        countState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(
            User user,
            Context ctx,
            Collector<User> out) throws Exception {

        Integer count = countState.value();

        if (count == null) {
            count = 0;
        }

        count++;

        countState.update(count);

        user.setCount(count);

        out.collect(user);
    }
}
