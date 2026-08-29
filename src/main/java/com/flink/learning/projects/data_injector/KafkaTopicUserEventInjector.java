package com.flink.learning.projects.data_injector;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.flink.learning.projects.FlinkJob;
import com.flink.learning.projects.data_injector.models.User;
import com.flink.learning.projects.serialization_helpers.UserSerializationSchema;

import jakarta.annotation.PostConstruct;

@Component
public class KafkaTopicUserEventInjector implements FlinkJob {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServer;

    @Value("${kafka.consumer.topic}")
    private String destinationTopic;

    @Value("${kafka.username}")
    private String username;

    @Value("${kafka.password}")
    private String password;

    private KafkaSink<User> sink;

    @PostConstruct
    public void init() {
        this.sink = KafkaSink.<User>builder()
            .setBootstrapServers(this.bootstrapServer)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(this.destinationTopic)
                    .setValueSerializationSchema(new UserSerializationSchema())
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
        return "user-event-generator";
    }

    @Override
    public void run() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String[][] users = {
            {"1", "John Doe"},
            {"2", "Jane Smith"},
            {"3", "Alice Johnson"},
            {"4", "Bob Brown"},
            {"5", "Charlie Davis"},
            {"6", "Eve Wilson"}
        };

        DataStream<User> stream = env.fromSequence(0, Long.MAX_VALUE)
            .map(i -> {
                Thread.sleep(5000); // emit every 5 second

                String[] u = users[ThreadLocalRandom.current().nextInt(users.length)];

                return new User(u[0], u[1]);
        });

        stream.sinkTo(sink);
        stream.print();

        env.execute("Kafka User Data Injector");
    }
}