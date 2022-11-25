package com.example.clientkafkabatch.core.config.batch;

import com.example.clientkafkabatch.core.config.kafka.JsonKafkaProducer;
import com.example.clientkafkabatch.domain.payload.Client;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ClientItemWriter implements ItemWriter<Client> {

    private final JsonKafkaProducer jsonKafkaProducer;

    public ClientItemWriter(JsonKafkaProducer jsonKafkaProducer) {
        this.jsonKafkaProducer = jsonKafkaProducer;
    }

    @Override
    public void write(List<? extends Client> list) throws Exception {
        //list.forEach(clientRepository::save);
        list.forEach(client -> jsonKafkaProducer.sendMessage(client));
    }
}
