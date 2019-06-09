package poststats.kafkaConsumer;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class EventDeserializer<T> implements Deserializer<T> {

    private Class<T> typeParameterClass;

    public EventDeserializer(Class<T> typeParameterClass) {
        this.typeParameterClass = typeParameterClass;
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public T deserialize(String s, byte[] arg1) {
        ObjectMapper mapper = new ObjectMapper();
        T obj = null;
        try {
            obj = mapper.readValue(arg1, typeParameterClass);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return obj;
    }

    @Override
    public void close() {

    }
}
