package com.flink.learning.projects.data_injector;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.flink.learning.projects.FlinkJob;
import com.flink.learning.projects.data_injector.models.User;
import com.flink.learning.projects.serialization_helpers.UserSerializationSchema;

import jakarta.annotation.PostConstruct;

@Component
public class KafkaTopicUserEventInjector implements FlinkJob {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTopicUserEventInjector.class);

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServer;

    @Value("${kafka.consumer.topic}")
    private String destinationTopic;

    private KafkaSink<User> sink;

    @Override
    public String name() {
        return "user-event-generator";
    }

    @PostConstruct
    public void init() {
        logger.info("Bootstrap: " + bootstrapServer);
        logger.info("Topic: " + destinationTopic);

        this.sink = KafkaSink.<User>builder()
            .setBootstrapServers(this.bootstrapServer)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(this.destinationTopic)
                    .setValueSerializationSchema(new UserSerializationSchema())
                    .build())
            .build();
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