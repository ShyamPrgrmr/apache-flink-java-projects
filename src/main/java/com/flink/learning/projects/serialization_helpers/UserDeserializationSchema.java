package com.flink.learning.projects.serialization_helpers;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink.learning.projects.data_injector.models.User;

public class UserDeserializationSchema implements DeserializationSchema<User> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public User deserialize(byte[] message) throws IOException {
        return mapper.readValue(message, User.class);
    }

    @Override
    public boolean isEndOfStream(User nextElement) {
        return false;
    }

    @Override
    public TypeInformation<User> getProducedType() {
        return TypeInformation.of(User.class);
    }
}