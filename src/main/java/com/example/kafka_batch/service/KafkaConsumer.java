package com.example.kafka_batch.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class KafkaConsumer {

    @KafkaListener(topics = "test-topic", containerFactory = "kafkaListenerContainerFactory")
    public void receive(Consumer<Long, String> consumer) {

        final int giveUp = 100;   int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

            System.out.println(consumerRecords.count());

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });

            consumer.commitAsync();
        }
        consumer.close();

//        System.out.println("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");
//        System.out.println("Starting the process to recieve batch messages");
//
//        System.out.println("Message Batch Size: " + messages.size());
//
//        for (String message: messages) {
//
//            System.out.println(message);
//
//        }
//        System.out.println("all the batch messages are consumed");
    }


}
