package com.flink.learning.projects.serialization_helpers;
import org.apache.flink.api.common.serialization.SerializationSchema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink.learning.projects.data_injector.models.User;

public class UserSerializationSchema implements SerializationSchema<User> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(User user) {
        try {
            return mapper.writeValueAsBytes(user);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}