package com.github.quartz.impl.redisjobstore.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class IntegerSetDeserializer extends JsonDeserializer<Object> {

    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        String daysOfWeekStr = p.getValueAsString();
        if (daysOfWeekStr == null || daysOfWeekStr.length() == 0) {
            return null;
        }
        String[] array = daysOfWeekStr.split(",");
        Set<Integer> daysOfWeekSet = new HashSet<>(array.length);
        for (String dayStr : array) {
            daysOfWeekSet.add(Integer.parseInt(dayStr));
        }
        return daysOfWeekSet;
    }

}
