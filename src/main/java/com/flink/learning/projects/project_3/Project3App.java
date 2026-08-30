package com.flink.learning.projects.project_3;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.flink.learning.projects.FlinkJob;
import com.flink.learning.projects.data_injector.models.CPU;
import com.flink.learning.projects.serialization_helpers.CPUDeserializationSchema;


@Component
public class Project3App implements FlinkJob{
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
        return "project3";
    }

    @Override
    public void run() throws Exception {
        KafkaSource<CPU> source = KafkaSource.<CPU>builder()
            .setBootstrapServers(this.bootstrapServer)
            .setTopics(this.sourceTopic)
            .setGroupId(this.groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setProperty("commit.offsets.on.checkpoint", "true")
            .setDeserializer(
                KafkaRecordDeserializationSchema.valueOnly(new CPUDeserializationSchema())
            )
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
        env.setParallelism(1);
        env.enableCheckpointing(10000); // Enable checkpointing every 10 seconds
            

        DataStream<CPU> userStream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );

        DataStream<String> averageCpu = userStream
            .keyBy(CPU::getName)
            .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
            .aggregate(new CpuAverageAggregator(), new CpuWindowResultFunction());

        //Sink_1
        averageCpu.print();

        // Sink_2
        averageCpu.sinkTo(sink);
        
        env.execute("Kafka CPU Usage Aggregation Example");
    }
}