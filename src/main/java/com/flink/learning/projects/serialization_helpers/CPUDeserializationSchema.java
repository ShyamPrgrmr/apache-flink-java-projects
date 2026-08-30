package com.flink.learning.projects.serialization_helpers;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink.learning.projects.data_injector.models.CPU;

public class CPUDeserializationSchema implements DeserializationSchema<CPU> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public CPU deserialize(byte[] message) throws IOException {
        return mapper.readValue(message, CPU.class);
    }

    @Override
    public boolean isEndOfStream(CPU nextElement) {
        return false;
    }

    @Override
    public TypeInformation<CPU> getProducedType() {
        return TypeInformation.of(CPU.class);
    }
}
