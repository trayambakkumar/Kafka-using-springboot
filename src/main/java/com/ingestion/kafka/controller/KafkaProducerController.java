package com.ingestion.kafka.controller;

import com.ingestion.kafka.models.Event;
import com.ingestion.kafka.service.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaProducerController {

    private final KafkaProducerService producerService;

    @Autowired
    public KafkaProducerController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    @GetMapping(value = "/createEvent")
    public void sendMessageToKafkaTopic(
            @RequestParam("eventId") long eventId,
            @RequestParam("eventName") String eventName) {

        Event event = new Event();
        event.setEventId(eventId);
        event.setEventName(eventName);

        this.producerService.saveCreateEventLog(event);
    }
}
