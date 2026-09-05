package com.flink.learning.projects.project_7;

import java.util.ArrayList;
import java.util.UUID;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Component;

import com.flink.learning.projects.FlinkJob;
import com.flink.learning.projects.data_injector.UPITransactionInjector;
import com.flink.learning.projects.data_injector.constants.Severity;
import com.flink.learning.projects.data_injector.models.Alert;
import com.flink.learning.projects.data_injector.models.UPIPayment;
import com.flink.learning.projects.data_injector.models.UPIThreshould;


@Component 
public class Project7App implements FlinkJob {
    
    private final Integer checkpointInterval = 10000; // 10 seconds

    @Override
    public String name() {
        return "project7";
    }

    @Override
    public void run() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(checkpointInterval); 


        DataStream<UPIPayment> upiPaymentsStream = env.fromCollection(UPITransactionInjector.createTestPayments()).assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());


        upiPaymentsStream.print("UPI PAYMENTS");

        ArrayList<UPIThreshould> thresholds = new ArrayList<>();

        thresholds.add(new UPIThreshould(
            UUID.randomUUID(),
            "to-user1@example.com",
            1000.0,
            "Threshold for user1",
            Severity.LOW
        ));

        thresholds.add(new UPIThreshould(
            UUID.randomUUID(),
            "to-user1@example.com",
            1000.0,
            "Threshold for user1",
            Severity.MEDIUM
        ));


        thresholds.add(new UPIThreshould(
            UUID.randomUUID(),
            "to-user1@example.com",
            5000.0,
            "Threshold for user1",
            Severity.HIGH
        ));

        DataStream<Alert> alertStream = upiPaymentsStream
            .keyBy(UPIPayment::getHandle)
            .process(new ProcessUPIPayments(thresholds))
            .name("UPI transactions alert stream")
            .uid("upi-transactions-alert-stream");

        alertStream
        .map(Alert::toString)
        .name("Alert to String")
        .print("ALERT");

        env.execute("Project 7 - Keyed State Demo");
    }
}
