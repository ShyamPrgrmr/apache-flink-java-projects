package com.flink.learning.projects.serialization_helpers;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.fasterxml.jackson.databind.ObjectMapper;


public class DeserializationFlink<V> implements DeserializationSchema<V> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    @SuppressWarnings("unchecked")
    public V deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, (Class<V>) ((java.lang.reflect.ParameterizedType) getClass()
                .getGenericSuperclass()).getActualTypeArguments()[0]);
    }

    @Override
    public boolean isEndOfStream(V nextElement) {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<V> getProducedType() {
        return TypeInformation.of((Class<V>) ((java.lang.reflect.ParameterizedType) getClass()
                .getGenericSuperclass()).getActualTypeArguments()[0]);
    }
}
