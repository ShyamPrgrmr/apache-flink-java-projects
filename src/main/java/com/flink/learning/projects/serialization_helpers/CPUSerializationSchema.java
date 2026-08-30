package com.flink.learning.projects.serialization_helpers;

import org.apache.flink.api.common.serialization.SerializationSchema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink.learning.projects.data_injector.models.CPU;

public class CPUSerializationSchema implements SerializationSchema<CPU> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(CPU cpu) {
        try {
            return mapper.writeValueAsBytes(cpu);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
