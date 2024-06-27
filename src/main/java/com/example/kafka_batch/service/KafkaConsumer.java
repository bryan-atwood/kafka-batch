package com.example.kafka_batch.service;

import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class KafkaConsumer {

    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    private LagAnalyzerService lagAnalyzerService;
    private Consumer consumer;

    public KafkaConsumer(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry, LagAnalyzerService lagAnalyzerService) {
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
        this.lagAnalyzerService = lagAnalyzerService;
    }

    @Scheduled(cron = "0 */2 * * * ?")
    public void scheduledJob(){
        System.out.println("Job Kicked Off!!!" + LocalDateTime.now());

        var startTime = LocalDateTime.now();
        var endTime = startTime.plusMinutes(1);

        System.out.println("Job Started!!!" + LocalDateTime.now());
        kafkaListenerEndpointRegistry.getListenerContainer("test-topic-id").start();

         while(LocalDateTime.now().isBefore(endTime)) {

             try {
                 Thread.sleep(5000);
             } catch (InterruptedException e) {
                 throw new RuntimeException(e);
             }

         }

        kafkaListenerEndpointRegistry.getListenerContainer("test-topic-id").stop();
        System.out.println("Job Stopped!!!" + LocalDateTime.now());

    }

    @KafkaListener(id = "test-topic-id", topics = "test-topic", containerFactory = "kafkaListenerContainerFactory", autoStartup = "false")
    public void receive(@Payload List<String> messages, Consumer consumer) {

        this.consumer = consumer;

        System.out.println("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");
        System.out.println("Starting the process to receive batch messages");

        System.out.println("Message Batch Size: " + messages.size());

        for (String message: messages) {

            System.out.println(message);

        }
        System.out.println("all the batch messages are consumed");
    }

}
