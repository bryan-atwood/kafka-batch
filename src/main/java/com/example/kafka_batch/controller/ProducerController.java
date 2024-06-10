package com.example.kafka_batch.controller;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;

@RestController
public class ProducerController {

    public KafkaTemplate kafkaTemplate;

    public ProducerController(KafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/produce")
    public void produce() {

        var headersMap = new HashMap<String, Object>();
        headersMap.put(KafkaHeaders.TOPIC, "test-topic");

        var headers = new MessageHeaders(headersMap);

        for (int i = 0; i < 500; i++) {
            var message = MessageBuilder.createMessage("Test Message " + i, headers);

            kafkaTemplate.send(message);
        }

    }
}
