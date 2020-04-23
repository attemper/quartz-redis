package com.github.quartz.impl.redisjobstore.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class ObjectSerializer extends JsonSerializer<Object> {

    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
        if (value != null) {
            ByteArrayOutputStream baos = serializeObject(value);
            gen.writeBinary(baos.toByteArray());
        }
    }

    /**
     * <p>
     * Create a serialized <code>java.util.ByteArrayOutputStream</code>
     * version of an Object.
     * </p>
     *
     * @param obj
     *          the object to serialize
     * @return the serialized ByteArrayOutputStream
     * @throws IOException
     *           if serialization causes an error
     */
    private ByteArrayOutputStream serializeObject(Object obj)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        if (null != obj) {
            ObjectOutputStream out = new ObjectOutputStream(baos);
            out.writeObject(obj);
            out.flush();
        }
        return baos;
    }
}
