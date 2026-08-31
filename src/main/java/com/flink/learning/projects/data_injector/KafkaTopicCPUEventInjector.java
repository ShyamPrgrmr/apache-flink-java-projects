package com.flink.learning.projects.data_injector;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.flink.learning.projects.FlinkJob;
import com.flink.learning.projects.data_injector.models.CPU;
import com.flink.learning.projects.serialization_helpers.CPUSerializationSchema;

import jakarta.annotation.PostConstruct;

@Component
public class KafkaTopicCPUEventInjector implements FlinkJob {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServer;

    @Value("${kafka.consumer.topic}")
    private String destinationTopic;

    @Value("${kafka.username}")
    private String username;

    @Value("${kafka.password}")
    private String password;

    private KafkaSink<CPU> sink;

    @PostConstruct
    public void init() {
        this.sink = KafkaSink.<CPU>builder()
            .setBootstrapServers(this.bootstrapServer)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(this.destinationTopic)
                    .setValueSerializationSchema(new CPUSerializationSchema())
                    .build())
            .setProperty("security.protocol", "SASL_PLAINTEXT")
            .setProperty("sasl.mechanism", "PLAIN")
            .setProperty(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"" + this.username + "\" password=\"" + this.password + "\";")
            .build();
    }

    @Override
    public String name() {
        return "cpu-event-generator";
    }

    @Override
    public void run() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        

    
        DataStream<CPU> stream = env.fromSequence(0, Long.MAX_VALUE)
        .flatMap((Long i, Collector<CPU> out) -> {
                Thread.sleep(500); // wait 0.5 sec

                out.collect(new CPU(
                    "EC2-CPU-1",
                    ThreadLocalRandom.current().nextDouble(0, 100),
                    System.currentTimeMillis()
                ));

                out.collect(new CPU(
                    "EC2-CPU-2",
                    ThreadLocalRandom.current().nextDouble(0, 100),
                    System.currentTimeMillis()
                ));
                long currentTime = System.currentTimeMillis();
                out.collect(new CPU(
                    "EC2-CPU-3",
                    ThreadLocalRandom.current().nextDouble(0, 100),
                    currentTime
                ));
        }).returns(CPU.class);

        stream.sinkTo(sink);
        stream.print();

        env.execute("Kafka CPU Data Injector");
    }
}