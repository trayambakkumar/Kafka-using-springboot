package com.ingestion.kafka.service;

import com.ingestion.kafka.models.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
    private final Logger logger
            = LoggerFactory.getLogger(KafkaConsumerService.class);

    @KafkaListener(
            topics = "${event.topic.name}",
            groupId = "${event.topic.group.id}",
            containerFactory = "eventKafkaListenerContainerFactory"
    )
    public void consume(Event event) {
        logger.info(String.format("Event created -> %s", event));
    }
}
