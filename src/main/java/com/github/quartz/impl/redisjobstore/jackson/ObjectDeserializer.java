package com.github.quartz.impl.redisjobstore.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class ObjectDeserializer extends JsonDeserializer<Object> {

    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        byte[] value = p.getBinaryValue();
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(value));
        try {
            return ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Read object error", e);
        }
    }

}
