package com.example.clientkafkabatch.core.config.kafka;

import com.example.clientkafkabatch.domain.payload.Client;
import com.example.clientkafkabatch.domain.payload.ClientRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class JsonKafkaConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonKafkaProducer.class);

    private final ClientRepository clientRepository;

    public JsonKafkaConsumer(ClientRepository clientRepository) {
        this.clientRepository = clientRepository;
    }

    @KafkaListener(topics = "json_goPartner", groupId = "myGroup" )/*  kafkaJsonListenerContainerFactory*/
    public void consume(Client client) {
        clientRepository.save(client);
        LOGGER.info(String.format("Json Message receiver -> %s", client.toString()));
    }

}
