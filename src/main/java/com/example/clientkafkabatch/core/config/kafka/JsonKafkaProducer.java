package com.example.clientkafkabatch.core.config.kafka;

import com.example.clientkafkabatch.domain.payload.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class JsonKafkaProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonKafkaProducer.class);

    private KafkaTemplate<String, Client> kafkaTemplate;


    public JsonKafkaProducer(KafkaTemplate<String, Client> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(Client client) {
        LOGGER.info(String.format("Message sent -> %s", client.toString() ));
        Message<Client> clientMessage = MessageBuilder
                .withPayload(client)
                .setHeader(KafkaHeaders.TOPIC, "json_goPartner")
                .build();
        kafkaTemplate.send(clientMessage);
    }
}
