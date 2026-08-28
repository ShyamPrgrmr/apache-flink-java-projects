package com.flink.learning.projects.project_1;

import java.util.Locale;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.flink.learning.projects.FlinkJob;

@Component
public class Project1App implements FlinkJob{
    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServer;

    @Value("${kafka.consumer.group-id}")
    private String groupId;

    @Value("${kafka.consumer.topic}")
    private String sourceTopic;

    @Value("${kafka.producer.topic}")
    private String destinationTopic;

    @Value("${kafka.username}")
    private String username;

    @Value("${kafka.password}")
    private String password;


    @Override
    public String name() {
        return "project1";
    }

    @Override
    public void run() throws Exception {
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(this.bootstrapServer)
            .setTopics(this.sourceTopic)
            .setGroupId(this.groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setProperty("commit.offsets.on.checkpoint", "true")
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setProperty("security.protocol", "SASL_PLAINTEXT")
            .setProperty("sasl.mechanism", "PLAIN")
            .setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"" + this.username + "\" password=\"" + this.password + "\";")
            .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers(this.bootstrapServer)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(this.destinationTopic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
            )
            .setProperty("security.protocol", "SASL_PLAINTEXT")
            .setProperty("sasl.mechanism", "PLAIN")
            .setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"" + this.username + "\" password=\"" + this.password + "\";")
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(10000); // Enable checkpointing every 10 seconds
            

        DataStream<String> stream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );

        DataStream<String> transformedStream = stream
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .map(value -> value.toUpperCase(Locale.ROOT));

        //Sink_1
        transformedStream.print();

        // Sink_2
        transformedStream.sinkTo(sink);
        
        env.execute("Kafka Consumer Example");
    }
}