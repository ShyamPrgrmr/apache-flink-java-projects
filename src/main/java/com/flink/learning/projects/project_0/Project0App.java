package com.flink.learning.projects.project_0;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.flink.learning.projects.FlinkJob;

@Component
public class Project0App implements FlinkJob {
    
    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServer;

    @Value("${kafka.consumer.group-id}")
    private String groupId;

    @Value("${kafka.consumer.topic}")
    private String sourceTopic;

    @Value("${kafka.username}")
    private String username;

    @Value("${kafka.password}")
    private String password;
    @Override
    public String name() {
        return "project0";
    }

    @Override
    public void run() throws Exception {
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServer)
            .setTopics(sourceTopic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStream<String> stream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );

        // Sink
        stream.print();

        
        env.execute("Kafka Consumer Example");
    }
}