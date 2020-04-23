package com.github.quartz.impl.redisjobstore.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.quartz.TimeOfDay;

import java.io.IOException;

public class TimeOfDaySerializer extends JsonSerializer<Object> {

    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
        if (value != null) {
            TimeOfDay timeOfDay = (TimeOfDay) value;
            StringBuilder s = new StringBuilder();
            s.append(timeOfDay.getHour())
                    .append(",")
                    .append(timeOfDay.getMinute())
                    .append(",")
                    .append(timeOfDay.getSecond());
                gen.writeString(s.toString());
        }
    }

}
