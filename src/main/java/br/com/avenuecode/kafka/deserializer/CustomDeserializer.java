package br.com.avenuecode.kafka.deserializer;

import br.com.avenuecode.kafka.entity.TruckCoordinates;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class CustomDeserializer implements Deserializer<TruckCoordinates> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public TruckCoordinates deserialize(String topic, byte[] bytes) {
        TruckCoordinates truckCoordinates = null;
        try{
            truckCoordinates = objectMapper.readValue(bytes, TruckCoordinates.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return truckCoordinates;
    }
}
