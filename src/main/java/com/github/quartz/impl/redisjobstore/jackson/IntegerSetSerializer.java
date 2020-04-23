package com.github.quartz.impl.redisjobstore.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.Set;

public class IntegerSetSerializer extends JsonSerializer<Object> {

    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
        if (value != null) {
            Set<Integer> daysOfWeekSet = (Set<Integer>) value;
            StringBuilder s = new StringBuilder();
            for (Integer i : daysOfWeekSet) {
                s.append(",").append(i);
            }
            if (s.length() > 0) {
                gen.writeString(s.substring(1));
            }
        }
    }

}
