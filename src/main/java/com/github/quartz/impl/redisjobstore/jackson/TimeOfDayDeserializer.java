package com.github.quartz.impl.redisjobstore.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.quartz.TimeOfDay;

import java.io.IOException;

public class TimeOfDayDeserializer extends JsonDeserializer<Object> {

    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        String timeOfDayStr = p.getValueAsString();
        if (timeOfDayStr == null || timeOfDayStr.length() == 0) {
            return null;
        }
        String[] array = timeOfDayStr.split(",");
        return TimeOfDay.hourMinuteAndSecondOfDay(
                Integer.parseInt(array[0]),
                Integer.parseInt(array[1]),
                Integer.parseInt(array[2]));
    }

}
