package com.ingestion.kafka.service;

import com.ingestion.kafka.constants.AppConstants;
import com.ingestion.kafka.models.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class KafkaProducerService {
    private static final Logger logger
            = LoggerFactory.getLogger(KafkaProducerService.class);

    @Value(value = "${event.topic.name}")
    private String topicName;

    private KafkaTemplate<String, Event> kafkaTemplate;

    @Autowired
    public KafkaProducerService(KafkaTemplate<String, Event> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void saveCreateEventLog(Event event) {
        logger.info(String.format("Event created -> %s", event));
        ListenableFuture<SendResult<String, Event>> future
                = this.kafkaTemplate.send(topicName, event);

        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable throwable) {
                logger.error("Event couldn't be sent " + event, throwable);
            }

            @Override
            public void onSuccess(SendResult<String, Event> stringObjectSendResult) {
                logger.info("Event created: " + event + " with offset " +
                        stringObjectSendResult.getRecordMetadata().offset());
            }
        });
    }
}
