package com.example.clientkafkabatch.core.config.batch;

import com.example.clientkafkabatch.domain.payload.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class ClientItemProcessor implements ItemProcessor<Client, Client> {

    private static final Logger log = LoggerFactory.getLogger(ClientItemProcessor.class);

    @Override
    public Client process(Client client) throws Exception {

        final Long id = client.getId();
        final String name = client.getName().toUpperCase();
        final String address = client.getAddress().toUpperCase();
        final String email = client.getEmail().toUpperCase();

        final Client clientTransformed = new Client(id, name, address, email);
        log.info("Converting (" + client + ") into (" + clientTransformed + ")");
        return clientTransformed;
    }
}
