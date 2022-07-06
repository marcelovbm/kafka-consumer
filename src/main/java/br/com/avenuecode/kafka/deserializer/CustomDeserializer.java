package br.com.avenuecode.kafka.deserializer;

import br.com.avenuecode.kafka.entity.TruckCoordinates;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class CustomDeserializer implements Deserializer<TruckCoordinates> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public TruckCoordinates deserialize(String topic, byte[] bytes) {
        TruckCoordinates record = null;
        try{
            record = objectMapper.readValue(bytes, TruckCoordinates.class);
        } catch (StreamReadException e) {
            e.printStackTrace();
        } catch (DatabindException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return record;
    }
}
